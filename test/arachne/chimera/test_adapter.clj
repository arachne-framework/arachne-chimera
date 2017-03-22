(ns arachne.chimera.test-adapter
  (:require [arachne.core.config :as cfg]
            [arachne.core.dsl :as a]
            [arachne.core.util :as util]
            [arachne.chimera :as chimera]
            [arachne.chimera.adapter :as adapter]
            [arachne.error :as err :refer [deferror error]]
            [clojure.spec :as s]
            [clojure.set :as set]
            [arachne.chimera.operation :as cho]))

(s/def :test.operation/foo any?)

(def ^:dynamic *global-data*)

(defn atom-store-ctor
  "Constructor for a new atom store"
  [this]
  (if (bound? #'*global-data*)
    *global-data*
    (atom {})))

(defn test-adapter
  "DSL to define a test adapter instance in the config."
  [migration]
  (let [capability (fn [op]
                     {:chimera.adapter.capability/operation {:chimera.operation/type op}
                      :chimera.adapter.capability/atomic? true})
        atomstore-tid (cfg/tempid)
        tid (cfg/tempid)]
    (cfg/with-provenance :test `test-adapter
      (a/transact
        [(util/mkeep
          {:db/id atomstore-tid
           :arachne.component/constructor ::atom-store-ctor})
         {:chimera.operation/type :test.operation/foo
          :chimera.operation/idempotent? false
          :chimera.operation/batchable? false}
         {:db/id tid
          :arachne.component/dependencies [{:arachne.component.dependency/key :atom
                                            :arachne.component.dependency/entity atomstore-tid}]
          :chimera.adapter/migrations [{:chimera.migration/name migration}]
          :chimera.adapter/capabilities (map capability [:chimera.operation/initialize-migrations
                                                         :chimera.operation/migrate
                                                         :chimera.operation/add-attribute
                                                         :chimera.operation/get
                                                         :chimera.operation/put
                                                         :chimera.operation/update
                                                         :chimera.operation/delete
                                                         :chimera.operation/delete-entity
                                                         :chimera.operation/batch
                                                         :test.operation/foo])
          :chimera.adapter/dispatches [{:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/initialize-migrations}
                                        :chimera.adapter.dispatch/impl ::init-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/migrate}
                                        :chimera.adapter.dispatch/impl ::migrate-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/put}
                                        :chimera.adapter.dispatch/impl ::put-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/get}
                                        :chimera.adapter.dispatch/impl ::get-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/update}
                                        :chimera.adapter.dispatch/impl ::update-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete-entity}
                                        :chimera.adapter.dispatch/impl ::delete-entity-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "[_ _ _]"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete}
                                        :chimera.adapter.dispatch/impl ::delete-attr-value-op}
                                       {:chimera.adapter.dispatch/index 1,
                                        :chimera.adapter.dispatch/pattern "[_ _]"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete}
                                        :chimera.adapter.dispatch/impl ::delete-attr-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/batch}
                                        :chimera.adapter.dispatch/impl ::batch-op}]}]
        tid))))

(defn- find-entity
  ([data lookup]
   (find-entity data (:attribute lookup) (:value lookup)))
  ([data attr value]
   (->> data
        (filter #(= value (get % attr)))
        first)))

(defn- entity-map-lookup
  "Given an entity map, return the identity lookup, or throw an exception if none exists."
  [adapter entity-map op]
  (let [key (first (filter #(adapter/key? adapter %) (keys entity-map)))]
    (when-not key
      (let [model-keys (adapter/key-attributes adapter)]
        (error ::cho/no-key-specified
               {:op op
                :adapter-eid (:db/id adapter)
                :adapter-aid (:arachne/id adapter)
                :provided-attrs model-keys
                :provided-attrs-str (err/bullet-list (keys entity-map))
                :key-attrs (adapter/key-attributes adapter)
                :key-attrs-str (err/bullet-list model-keys)})))
    [key (entity-map key)]))

(defn- datastore
  "Retrieve the datastore for modification"
  [adapter]
  (:atom adapter))

(defn- ensure-ref-values
  "Given an entity map, ensure that all lookup values exist in the data store, throwing an error otherwise"
  [adapter op data entity-map]
  (doseq [[attr value] entity-map]
    (when (adapter/ref? adapter attr)
      (let [card-many? (adapter/cardinality-many? adapter attr)]
        (when (and card-many? (not (set? value)))
          (error ::cho/unexpected-cardinality-one
                 {:attribute attr
                  :value value
                  :op op
                  :adapter-eid (:db/id adapter)
                  :adapter-aid (:arachne/id adapter)}))
        (when (and (set? value) (not card-many?))
          (error ::cho/unexpected-cardinality-many
                 {:attribute attr
                  :value value
                  :op op
                  :adapter-eid (:db/id adapter)
                  :adapter-aid (:arachne/id adapter)}))
        (doseq [lu (if card-many? value [value])]
          (when-not (find-entity (:data data) lu)
            (error ::cho/entity-does-not-exist
                   {:lookup [(:attribute lu) (:value lu)]
                    :op op
                    :adapter-eid (:db/id adapter)
                    :adapter-aid (:arachne/id adapter)})))))))

(defn put-op
  ([adapter op-type emap]
   (let [ds (datastore adapter)]
     (swap! ds
            (fn [data]
              (put-op adapter op-type emap data))))
   true)
  ([adapter _ emap data]
   (let [[k v] (entity-map-lookup adapter emap :chimera.operation/put)
         existing (when k (find-entity (:data data) k v))]
     (if existing
       (error ::cho/entity-already-exists
              {:lookup [k v]
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})
       (do
         (ensure-ref-values adapter :chimera.operation/put data emap)
         (update-in data [:data] (fnil conj #{}) emap))))))

(defn- simple-coll? [x] (and (coll? x) (not (map? x))))

(defn- update-merge
  "Merge fn for updates"
  [old new]
  (if (and (simple-coll? old) (simple-coll? new))
    (set (concat old new))
    new))

(defn- update-entity
  "Update an item in the data store"
  [entities entity update-map]
  (let [entities (disj entities entity)
        updated-entity (merge-with update-merge entity update-map)]
    (conj entities updated-entity)))

(defn update-op
  ([adapter op-type emap]
   (swap! (datastore adapter)
          (fn [data] (update-op adapter op-type emap data)))
   true)
  ([adapter op-type emap data]
   (let [[k v] (entity-map-lookup adapter emap :chimera.operation/update)
         existing (when k (find-entity (:data data) k v))]
     (if existing
       (do
         (ensure-ref-values adapter :chimera.operation/update data emap)
         (update data :data update-entity existing emap))
       (error ::cho/entity-does-not-exist
              {:lookup [k v]
               :op :chimera.operation/update
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})))))

(defn- remove-entity-references
  "Remove all ref attributes targeting the specified lookup"
  [data entity adapter]
  (let [lu (chimera/lookup (entity-map-lookup adapter entity :arachne.operation/delete-entity))]
    (set (map (fn [entity]
           (into {} (map (fn [[k v :as entry]]
                           (if (set? v)
                             [k (disj v lu)]
                             (if (= v lu) nil entry)))
                      entity)))
      data))))

(defn- delete-entity
  "Delete the given entity from the data store"
  [data entity adapter]
  (let [component-lookups (mapcat (fn [[k v]]
                                    (when (adapter/component? adapter k)
                                      (if (set? v) v #{v})))
                                entity)
        components (map (fn [lookup]
                          (find-entity data lookup))
                        component-lookups)]
    (reduce (fn [data entity]
              (delete-entity data entity adapter))
      (-> data
        (disj entity)
        (remove-entity-references entity adapter))
      components)))

(defn delete-entity-op
  ([adapter op-type lookup]
   (swap! (datastore adapter)
          (fn [data] (delete-entity-op adapter op-type lookup data)))
   true)
  ([adapter op-type lookup data]
   (let [entity (find-entity (:data data) lookup)]
     (if entity
       (update data :data delete-entity entity adapter)
       (error ::cho/entity-does-not-exist
              {:lookup [(:attribute lookup) (:value lookup)]
               :op :chimera.operation/delete-entity
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})))))

(defn- delete-attr
  [data entity attr adapter]
  (let [new-entity (dissoc entity attr)
        new-data (conj (disj data entity) new-entity)
        cleaned-data (if (and (not= entity new-entity) (adapter/component? adapter attr))
                       (let [entities-to-remove (if (adapter/cardinality-many? adapter attr)
                                         (get entity attr)
                                         [(get entity attr)])]
                         (reduce (fn [data to-remove]
                                   (delete-entity data (find-entity data to-remove) adapter))
                           new-data
                           entities-to-remove))
                       new-data)]
    cleaned-data))

(defn delete-attr-op
  ([adapter op-type payload]
   (swap! (datastore adapter)
          (fn [data] (delete-attr-op adapter op-type payload data)))
   true)
  ([adapter op-type [lookup attr] data]
   (when (adapter/key? adapter attr)
     (error ::cho/cannot-delete-key-attr {:lookup lookup
                                          :attribute attr
                                          :adapter-eid (:db/id adapter)
                                          :adapter-aid (:arachne/id adapter)}))
   (let [entity (find-entity (:data data) lookup)]
     (if entity
       (update data :data delete-attr entity attr adapter)
       (error ::cho/entity-does-not-exist
              {:lookup [(:attribute lookup) (:value lookup)]
               :op :chimera.operation/delete
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})))))

(defn- delete-attr-value
  [data entity attr value adapter]
  (let [new-entity (into {} (filter (fn [[k v]]
                                      (if (= attr k)
                                        (if (set? v)
                                          [k (disj v value)]
                                          (if (= value v)
                                            nil
                                            [k v]))
                                        [k v]))
                                    entity))
        new-data (conj (disj data entity) new-entity)
        cleaned-data (if (and (not= entity new-entity) (adapter/component? adapter attr))
                       (delete-entity data (find-entity data value) adapter)
                       new-data)]
    cleaned-data))

(defn delete-attr-value-op
  ([adapter op-type payload]
   (swap! (datastore adapter)
          (fn [data] (delete-attr-value-op adapter op-type payload data)))
   true)
  ([adapter op-type [lookup attr value] data]
   (let [entity (find-entity (:data data) lookup)]
     (if entity
       (update data :data delete-attr-value entity attr value adapter)
       (error ::cho/entity-does-not-exist
              {:lookup [(:attribute lookup) (:value lookup)]
               :op :chimera.operation/delete
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})))))

(defn- realize-components
  "Given an entity map, recursively replace all component lookups with entity maps"
  [adapter data entity]
  (let [realize-component (fn [lookup]
                            (realize-components adapter data
                              (find-entity data (:attribute lookup) (:value lookup))))
        entries (map (fn [[k v]]
                       (if (adapter/component? adapter k)
                         (if (adapter/cardinality-many? adapter k)
                           [k (set (map realize-component v))]
                           [k (realize-component v)])
                         [k v]))
                  entity)]

    (when (seq entries)
      (into {} entries))))

(defn get-op
  [adapter _ lookup]
  (let [ds @(datastore adapter)]
    (let [entity (find-entity (:data ds) (:attribute lookup) (:value lookup))]
      (realize-components adapter (:data ds) entity))))

(defn init-op
  [adapter _ _]
  true)

(defn migrate-op
  [adapter _ [signature migration]]
  (let [name (:chimera.migration/name migration)]
    (swap! (datastore adapter) update-in [:migrations name]
      (fn [[original-sig date]]
        (if original-sig
          (if (= original-sig signature)
            [original-sig date]
            (error :arachne.chimera.migration/invalid-signature
                   {:name name
                    :adapter-eid (:db/id adapter)
                    :adapter-aid (:arachne/id adapter)
                    :original-time date
                    :original-time-str (err/format-date date)
                    :original original-sig
                    :new signature}))
          [signature (java.util.Date.)])))))

(defn migrate-op
  [adapter _ [signature migration]]
  (let [name (:chimera.migration/name migration)]
    (swap! (datastore adapter) update-in [:migrations name]
           (fn [[original-sig date]]
             (if original-sig
               (if (= original-sig signature)
                 [original-sig date]
                 (error :arachne.chimera.migration/invalid-signature
                        {:name name
                         :adapter-eid (:db/id adapter)
                         :adapter-aid (:arachne/id adapter)
                         :original-time date
                         :original-time-str (err/format-date date)
                         :original original-sig
                         :new signature}))
               [signature (java.util.Date.)])))))

(defn batch-op
  [adapter _ payload]
  (swap! (datastore adapter)
         (fn [data]
           (reduce (fn [data [op-type payload]]
                     (chimera/operate adapter op-type payload data))
                   data payload))))