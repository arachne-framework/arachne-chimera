(ns arachne.chimera.test-adapter
  (:require [arachne.core.config :as cfg]
            [arachne.core.dsl :as a]
            [arachne.error :refer [deferror error format-date]]
            [clojure.spec :as s]
            [clojure.set :as set]
            [arachne.chimera.operation :as cho]))

(s/def :test.operation/foo any?)

(def ^:dynamic *data*)

(defn test-adapter
  "DSL to define a test adapter instance in the config."
  [migration]
  (let [capability (fn [[op i]]
                     {:chimera.adapter.capability/operation op
                      :chimera.adapter.capability/idempotent i
                      :chimera.adapter.capability/transactional true
                      :chimera.adapter.capability/atomic true})
        tid (cfg/tempid)]
    (cfg/with-provenance :test `test-adapter
      (a/transact
        [{:db/id tid
          :chimera.adapter/migrations [{:chimera.migration/name migration}]
          :chimera.adapter/capabilities (map capability
                                          [[:chimera.operation/initialize-migrations true]
                                           [:chimera.operation/migrate true]
                                           [:chimera.operation/add-attribute true]
                                           [:chimera.operation/get true]
                                           [:chimera.operation/put false]
                                           [:chimera.operation/update true]
                                           [:chimera.operation/delete true]
                                           [:chiemra.operation/batch false]
                                           [:test.operation/foo true]])
          :chimera.adapter/dispatches [{:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/migrate _]",
                                        :chimera.adapter.dispatch/impl ::migrate-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/initialize-migrations _]",
                                        :chimera.adapter.dispatch/impl ::init-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/put _]",
                                        :chimera.adapter.dispatch/impl ::put-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/get _]",
                                        :chimera.adapter.dispatch/impl ::get-op}
                                       ]}]
        tid))))

(defn- model-keys
  "Return a set of primary keys in the adapter's model"
  [adapter]
  (set
    (cfg/q (:arachne/config adapter)
      '[:find [?key ...]
        :in $ ?adapter
        :where
        [?adapter :chimera.adapter/model ?attr]
        [?attr :chimera.attribute/key true]
        [?attr :chimera.attribute/name ?key]]
      (:db/id adapter))))

(defn- find-entity
  [data  attr value]
  (->> data
       :data
       (filter #(= value (get % attr)))
       first))

(defn put-op
  [adapter _ emap]
  (let [[k v] (first (select-keys emap (model-keys adapter)))]
    (swap! *data*
      (fn [data]
        (let [existing (when k (find-entity data k v))]
          (if existing
            (error ::cho/entity-already-exists
              {:lookup [k v]
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)})
            (update-in data [:data] (fnil conj #{}) emap))))))
  true)

(defn get-op
  [adapter _ lookup]
  (find-entity @*data* (:attribute lookup) (:value lookup)))

(defn init-op
  [adapter _ _]
  true)

(defn migrate-op
  [adapter _ [signature migration]]
  (let [name (:chimera.migration/name migration)]
    (swap! *data* update-in [:migrations name]
      (fn [[original-sig date]]
        (if original-sig
          (if (= original-sig signature)
            [original-sig date]
            (error :arachne.chimera.migration/invalid-signature
              {:name name
               :adapter-eid (:db/id adapter)
               :adapter-aid (:arachne/id adapter)
               :original-time date
               :original-time-str (str date (format-date date))
               :original original-sig
               :new signature}))
          [signature (java.util.Date.)])))))

(comment
  ;;; Example DB
  {
   :migrations {:test/m1 ["abcdef" 1023402342423]}
   :data #{{:foo/bar 3}
           {:foo/bar 2}}
   }


  )