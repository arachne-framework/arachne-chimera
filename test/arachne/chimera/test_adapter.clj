(ns arachne.chimera.test-adapter
  (:require [arachne.core.config :as cfg]
            [arachne.core.dsl :as c]
            [arachne.error :refer [deferror error format-date]]
            [clojure.spec :as s]))

(s/def :test.operation/foo any?)

(def ^:dynamic *data*)

(defn test-adapter
  "DSL to define a test adapter instance in the config."
  [arachne-id migration]
  (let [capability (fn [op]
                     {:chimera.adapter.capability/operation op
                      :chimera.adapter.capability/idempotent true
                      :chimera.adapter.capability/transactional true
                      :chimera.adapter.capability/atomic true})]
    (cfg/with-provenance :test `test-adapter
      (c/transact
        [{:arachne/id arachne-id
          :chimera.adapter/migrations [{:chimera.migration/name migration}]
          :chimera.adapter/capabilities (map capability
                                          [:chimera.operation/initialize-migrations
                                           :chimera.operation/migrate
                                           :chimera.operation/add-attribute
                                           :chimera.operation/get
                                           :chimera.operation/put
                                           :chimera.operation/update
                                           :chimera.operation/delete
                                           :chiemra.soperation/batch
                                           :test.operation/foo])
          :chimera.adapter/dispatches [{:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/migrate _]",
                                        :chimera.adapter.dispatch/impl ::migrate-op}
                                       {:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/initialize-migrations _]",
                                        :chimera.adapter.dispatch/impl ::init-op}]}]))))

(defn init-op
  [adapter _ _]
  )


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
               :original-time-str (format-date date)
               :original original-sig
               :new signature}))
          [signature (Date.)])))))

(comment
  ;;; Example DB
  {:migrations {:test/m1 ["abcdef" 1023402342423]}}


  )