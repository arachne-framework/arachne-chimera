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
            [com.stuartsierra.component :as component]
            [arachne.chimera.test-adapter :as ta])
  (:import [arachne ArachneException]
           (java.util UUID Date)))

(defn test-config
  "DSL function to buidl a test config"
  [dob-min-card]

  (ch/migration :test/m1
    "Migration to set up schema for example-based tests"
    []
    (ch/attr :test.person/id :test/Person :uuid :min 1 :max 1)
    (ch/attr :test.person/name :test/Person :string :min 1 :max 1))
  (ch/migration :test/m2
    "Migration to set up schema for example-based tests"
    [:test/m1]
    (ch/attr :test.person/dob :test/Person :instant :min dob-min-card :max 1)
    (ch/attr :test.person/friends :test/Person :ref :test/Person :min 0))

  (a/id :test/adapter (ta/test-adapter :test/m2))

  (a/id :test/rt (a/runtime [:test/adapter]))

  )

(deftest adapter-migrations
  (binding [ta/*data* (atom {})]

    (testing "initial migration"
      (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                  '(arachne.chimera.adapter-test/test-config 0))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)]
        (chimera/ensure-migrations rt [:arachne/id :test/adapter]))

      (is (= 3 (count (:migrations @ta/*data*)))))

    (testing "idempotency"
      (let [before @ta/*data*
            cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                  '(arachne.chimera.adapter-test/test-config 0))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)]
        (chimera/ensure-migrations rt [:arachne/id :test/adapter])
        (is (= before @ta/*data*))))

    (testing "checksums"
      (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                  '(arachne.chimera.adapter-test/test-config 1))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)]
        (is (thrown-with-msg? ArachneException #"checksum"
              (chimera/ensure-migrations rt [:arachne/id :test/adapter])))))

    ))

(deftest bad-operations
  (binding [ta/*data* (atom {})]
    (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                '(arachne.chimera.adapter-test/test-config 0))
          rt (rt/init cfg [:arachne/id :test/rt])
          rt (component/start rt)
          adapter (rt/lookup rt [:arachne/id :test/adapter])]
      (is (thrown-with-msg? ArachneException #"not supported"
            (chimera/operate adapter :no.such/operation [])))
      (is (thrown-with-msg? ArachneException #"Unknown dispatch"
            (chimera/operate adapter :test.operation/foo [])))
      (is (thrown-with-msg? ArachneException #"did not conform "
            (chimera/operate adapter :chimera.operation/initialize-migrations false))))))


(deftest simple-crud
  (binding [ta/*data* (atom {})]
    (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                '(arachne.chimera.adapter-test/test-config 0))
          rt (rt/init cfg [:arachne/id :test/rt])
          rt (component/start rt)
          adapter (rt/lookup rt [:arachne/id :test/adapter])]
      (let [james (UUID/randomUUID)
            mary (UUID/randomUUID)]

        (testing "basic put/get"
          (chimera/operate adapter :chimera.operation/put
            [:test/Person {:test.person/id james
                           :test.person/name "James"}])

          ;; Todo: finish implementing with tests for basic defined behavior of put/get/update/delete, including error cases.

          #_(chimera/operate adapter :chimera.operation/put
            [:test/Person {:test.person/id mary
                           :test.person/name "Mary"}])
          #_(is (= #{{:test.person/id james, :test.person/name "James"}}
                (chimera/operate adapter :chimera.operation/get
                  [:test/Person [:test.person/id james]])))
          #_(is (= #{{:test.person/id mary, :test.person/name "Mary"}}
                (chimera/operate adapter :chimera.operation/get
                  [:test/Person [:test.person/id mary]])))
          #_(is (= #{}
                (chimera/operate adapter :chimera.operation/get
                  [:test/Person [:test.person/id (UUID/randomUUID)]]))))

        #_(testing "error on multiple puts"
          (is (thrown-with-msg? ArachneException #"already"
                (chimera/operate adapter :chimera.operation/put
                  [:test/Person {:test.person/id james
                                 :test.person/name "James"}]))))

        )

      )))

(deftest batch
  (binding [ta/*data* (atom {})]
    (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                '(arachne.chimera.adapter-test/test-config 0))
          rt (rt/init cfg [:arachne/id :test/rt])
          rt (component/start rt)
          adapter (rt/lookup rt [:arachne/id :test/adapter])]

      ;; TODO: test batch operations, including tests for atomicity and transactionality.

      )))

;; TODO: refactor so these tests can be applied as a battery, to any adapter, not just the test adapter.
;; TODO: Write Datomic, K/V & SQL adapters, and apply these tests
