(ns arachne.chimera.adapter-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [arachne.core.dsl :as c]
            [arachne.core.config :as core-cfg]
            [arachne.chimera :as chimera]
            [arachne.chimera.dsl :as ch]
            [arachne.chimera.adapter :as a]
            [com.stuartsierra.component :as component])
  (:import [arachne ArachneException]))

(s/def :test.operation/foo any?)

(defn adapter-test-migrations
  []
  "Intended to be called from within a config script. Will create migrations for
  the basic example-based adapter tests."
  (ch/migration :test/adapter-examples
    "Migration to set up schema for example-based tests"
    []
    (ch/attr :test.person/id :test/Person :uuid :min 1 :max 1)
    (ch/attr :test.person/name :test/Person :string :min 1 :max 1)))

(defn test-adapter
  "Define a test adapter instance in the config. Inteded to be called from
  within a config script."
  [arachne-id]
  (adapter-test-migrations)
  (let [capability (fn [op]
                     {:chimera.adapter.capability/operation op
                      :chimera.adapter.capability/idempotent true
                      :chimera.adapter.capability/transactional true
                      :chimera.adapter.capability/atomic true})]
    (core-cfg/with-provenance :test `test-adapter
      (c/transact
        [{:arachne/id arachne-id
          :chimera.adapter/migrations [{:chimera.migration/name
                                        :test/adapter-examples}]
          :chimera.adapter/capabilities (map capability
                                          [:chimera.operation/migrate
                                           :chimera.operation/add-attribute
                                           :chimera.operation/get
                                           :chimera.operation/put
                                           :chimera.operation/update
                                           :chimera.operation/delete
                                           :chiemra.soperation/batch
                                           :test.operation/foo])
          :chimera.adapter/dispatches [{:chimera.adapter.dispatch/index 0,
                                        :chimera.adapter.dispatch/pattern
                                        "[:chimera.operation/get _]",
                                        :chimera.adapter.dispatch/impl ::get-op}]}]))))

(defn get-op
  [adapter _ payload]
  42)

(defn adapter-example
  "Run some example-based assertions on the specified adapter. The basic four
  operations will be tested:

  :chimera.operation/get
  :chimera.operation/put
  :chimera.operation/update
  :chimera.operation/delete

  As well as compositions of these using :chimera.operation/batch.

  The adapter should support the :test/adapter-examples migration, which can be
  added using the `adapter-test-migrations` function from this namespace"
  [adapter]
  (is (= 0 1))

  )

(deftest basic-adapter
  (let [cfg (core/build-config '[:org.arachne-framework/arachne-chimera]
               '(do (require '[arachne.core.dsl :as a])
                    (require '[arachne.chimera.dsl :as c])
                    (require '[arachne.chimera.adapter-test :as at])

                    (at/test-adapter :test/adapter)

                    (a/runtime :test/rt [:test/adapter])))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]
    (is (thrown-with-msg? ArachneException #"Unknown dispatch"
          (chimera/operate adapter :test.operation/foo [])))

    (is (= 42
          (chimera/operate adapter :chimera.operation/get [:test.person/id 42])))

    )
  )
