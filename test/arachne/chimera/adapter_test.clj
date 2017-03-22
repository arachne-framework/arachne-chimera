(ns arachne.chimera.adapter-test
  "These tests form the reference implementation for any Adapter."
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [arachne.core.dsl :as a]
            [arachne.core.config :as core-cfg]
            [arachne.chimera :as chimera]
            [arachne.chimera.dsl :as ch]
            [arachne.chimera.adapter :as ca]
            [arachne.chimera.operation :as op]
            [arachne.chimera.test-harness :as harness]
            [com.stuartsierra.component :as component]
            [arachne.chimera.test-adapter :as ta])
  (:import [arachne ArachneException]
           (java.util UUID Date)))

(deftest test-harness
  (harness/exercise-all ta/test-adapter [:org.arachne-framework/arachne-chimera]))

(defn config
  "DSL function to build a test config"
  [dob-min-card]

  (ch/migration :test/m1
                "Migration to set up schema for example-based tests"
                []
                (ch/attr :test.person/id :test/Person :key :uuid :min 1 :max 1)
                (ch/attr :test.person/name :test/Person :string :min 1 :max 1))
  (ch/migration :test/m2
                "Migration to set up schema for example-based tests"
                [:test/m1]
                (ch/attr :test.person/dob :test/Person :instant :min dob-min-card :max 1)
                (ch/attr :test.person/friends :test/Person :ref :test/Person :min 0))

  (a/id :test/adapter (ta/test-adapter :test/m2))

  (a/id :test/rt (a/runtime [:test/adapter])))

;; These are adapter specific
(deftest adapter-migrations
  (testing "initial migration"
    (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                                 `(config 0))
          rt (rt/init cfg [:arachne/id :test/rt])
          rt (component/start rt)]
      (chimera/ensure-migrations rt [:arachne/id :test/adapter])

      (is (= 3 (count (:migrations @(:atom (rt/lookup rt [:arachne/id :test/adapter]))))))))

  (binding [ta/*global-data* (atom {})]

    (testing "idempotency"
      (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                                   `(config 0))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)
            _ (chimera/ensure-migrations rt [:arachne/id :test/adapter])
            before @(:atom (rt/lookup rt [:arachne/id :test/adapter]))
            _ (chimera/ensure-migrations rt [:arachne/id :test/adapter])]
        (is (= before @(:atom (rt/lookup rt [:arachne/id :test/adapter]))))))

    (testing "checksums"
      (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                                   `(config 1))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)]
        (is (thrown-with-msg? ArachneException #"checksum"
                              (chimera/ensure-migrations rt [:arachne/id :test/adapter])))))))

;; TODO: Write Datomic, K/V & SQL adapters, and apply these tests
