(ns arachne.chimera.adapter-test
  "These tests form the reference implementation for any Adapter."
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as st]
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

(arachne.error/explain-test-errors!)

;(st/instrument)
(alter-var-root #'arachne.chimera/*validate-operation-results* (constantly true))

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

;; These are adapter specific, so not in the general test harness
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

(deftest adapter-start-stop
  (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera] `(config 0))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)]

    (is (= 42 (:state (rt/lookup rt [:arachne/id :test/adapter]))))

    (let [stopped-rt (component/stop rt)]
      (is (= 43 (:state (rt/lookup stopped-rt [:arachne/id :test/adapter])))))))

(deftest adapter-utilities
  (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera] `(config 0))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (is (= #{:test.person/id
             :test.person/name
             :test.person/dob
             :test.person/friends}
          (set (map :chimera.attribute/name (ca/attrs-for-type adapter :test/Person)))
          (set (map :chimera.attribute/name
                 (ca/attrs-for-entity adapter
                   (chimera/lookup :test.person/id (UUID/randomUUID)))))))

    (component/stop rt)))
