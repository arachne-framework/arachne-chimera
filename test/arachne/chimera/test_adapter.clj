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
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete}
                                        :chimera.adapter.dispatch/impl ::delete-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern "_"
                                        :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/batch}
                                        :chimera.adapter.dispatch/impl ::batch-op}]}]
        tid))))

(defn- find-entity
  [data  attr value]
  (->> data
       :data
       (filter #(= value (get % attr)))
       first))

(def model-keys (adapter/weak-memoize adapter/model-keys))

(defn- entity-map-lookup
  "Given an entity map, return the identity lookup, or throw an exception if none exists."
  [adapter entity-map op]
  (let [model-keys (model-keys adapter)
        lookup (first (select-keys entity-map model-keys))]
    (when-not lookup
      (error ::cho/no-key-specified
             {:op op
              :adapter-eid (:db/id adapter)
              :adapter-aid (:arachne/id adapter)
              :provided-attrs (keys entity-map)
              :provided-attrs-str (err/bullet-list (keys entity-map))
              :key-attrs model-keys
              :key-attrs-str (err/bullet-list model-keys)}))
    lookup))

(defn- datastore
  "Retrieve the datastore for modification"
  [adapter]
  (:atom adapter))

(defn put-op
  ([adapter op-type emap]
   (let [ds (datastore adapter)]
     (swap! ds
            (fn [data]
              (put-op adapter op-type emap data))))
   true)
  ([adapter _ emap data]
   (let [[k v] (entity-map-lookup adapter emap :chimera.operation/put)
         existing (when k (find-entity data k v))]
     (if existing
       (error ::cho/entity-already-exists
              {:lookup [k v]
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})
       (update-in data [:data] (fnil conj #{}) emap)))))

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
         existing (when k (find-entity data k v))]
     (if existing
       (update data :data update-entity existing emap)
       (error ::cho/entity-does-not-exist
              {:lookup [k v]
               :op :chimera.operation/update
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})))))

(def component-attrs (adapter/weak-memoize adapter/component-attrs))

(defn- delete-entity
  "Delete the given entity from the data store"
  [data entity component-attrs]
  (let [component-values (select-keys entity component-attrs)
        component-lookups (apply concat (vals component-values))
        components (map (fn [[attr val]]
                          (find-entity data attr val))
                        component-lookups)]
    (reduce delete-entity (disj data entity) components)))

(defn delete-op
  ([adapter op-type lookup]
   (swap! (datastore adapter)
          (fn [data] (delete-op adapter op-type lookup data)))
   true)
  ([adapter op-type lookup data]
   (let [{:keys [attribute value]} lookup
         entity (find-entity data attribute value)]
     (if entity
       (update data :data delete-entity entity (component-attrs adapter))
       (error ::cho/entity-does-not-exist
              {:lookup [attribute value]
               :op :chimera.operation/delete
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})))))

(defn get-op
  [adapter _ lookup]
  (find-entity @(datastore adapter) (:attribute lookup) (:value lookup)))

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

(comment
  ;;; Example DB
  {
   :migrations {:test/m1 ["abcdef" 1023402342423]}
   :data #{{:foo/bar 3}
           {:foo/bar 2}}
   }

  )