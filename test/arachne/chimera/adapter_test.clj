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
            [com.stuartsierra.component :as component]
            [arachne.chimera.test-adapter :as ta])
  (:import [arachne ArachneException]
           (java.util UUID Date)))

(defn test-config
  "DSL function to buidl a test config"
  ([dob-min-card] (test-config dob-min-card nil))
  ([dob-min-card atomstore-aid]

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

   (a/id :test/adapter (ta/test-adapter :test/m2 atomstore-aid))

   (a/id :test/rt (a/runtime [:test/adapter]))))

(deftest adapter-migrations
  (testing "initial migration"
    (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                                 '(arachne.chimera.adapter-test/test-config 0))
          rt (rt/init cfg [:arachne/id :test/rt])
          rt (component/start rt)]
      (chimera/ensure-migrations rt [:arachne/id :test/adapter])

      (is (= 3 (count (:migrations @(:atom (rt/lookup rt [:arachne/id :test/adapter]))))))))

  (binding [ta/*global-data* (atom {})]

    (testing "idempotency"
      (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                                   '(arachne.chimera.adapter-test/test-config 0))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)
            _ (chimera/ensure-migrations rt [:arachne/id :test/adapter])
            before @(:atom (rt/lookup rt [:arachne/id :test/adapter]))
            _ (chimera/ensure-migrations rt [:arachne/id :test/adapter])]
        (is (= before @(:atom (rt/lookup rt [:arachne/id :test/adapter]))))))

    (testing "checksums"
      (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                                   '(arachne.chimera.adapter-test/test-config 1))
            rt (rt/init cfg [:arachne/id :test/rt])
            rt (component/start rt)]
        (is (thrown-with-msg? ArachneException #"checksum"
                              (chimera/ensure-migrations rt [:arachne/id :test/adapter]))))))
  )

(deftest bad-operations
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
                          (chimera/operate adapter :chimera.operation/initialize-migrations false)))))


(deftest simple-crud
  (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                               '(arachne.chimera.adapter-test/test-config 0))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]
    (let [james (UUID/randomUUID)
          mary (UUID/randomUUID)]

      (testing "basic put/get"
        (chimera/operate adapter :chimera.operation/put {:test.person/id james
                                                         :test.person/name "James"})
        (chimera/operate adapter :chimera.operation/put {:test.person/id mary
                                                         :test.person/name "Mary"})
        (is (= {:test.person/id james, :test.person/name "James"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))
        (is (= {:test.person/id mary, :test.person/name "Mary"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id mary))))
        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id (UUID/randomUUID)))))
        (is (thrown-with-msg? ArachneException #"already"
                              (chimera/operate adapter :chimera.operation/put {:test.person/id james
                                                                               :test.person/name "James"})))
        (is (thrown-with-msg? ArachneException #"requires a key"
                              (chimera/operate adapter :chimera.operation/put {:test.person/name "Elizabeth"}))))

      (testing "update"
        (let [t1 (java.util.Date.)
              t2 (java.util.Date.)]

          (chimera/operate adapter :chimera.operation/update {:test.person/id james
                                                              :test.person/dob t1})

          (is (= {:test.person/id james,
                  :test.person/name "James"
                  :test.person/dob t1}
                 (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

          (is (thrown-with-msg? ArachneException #"does not exist"
                                (chimera/operate adapter :chimera.operation/update {:test.person/id (UUID/randomUUID)
                                                                                    :test.person/dob t1})))

          (is (thrown-with-msg? ArachneException #"requires a key"
                                (chimera/operate adapter :chimera.operation/update {:test.person/dob t1})))


          (chimera/operate adapter :chimera.operation/update {:test.person/id james
                                                              :test.person/name "Jimmy"
                                                              :test.person/dob t2})

          (is (= {:test.person/id james,
                  :test.person/name "Jimmy"
                  :test.person/dob t2}
                 (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))))

      (testing "delete"

        (chimera/operate adapter :chimera.operation/delete (chimera/lookup :test.person/id james))

        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (is (thrown-with-msg? ArachneException #"does not exist"
                              (chimera/operate adapter :chimera.operation/delete (chimera/lookup :test.person/id james))))

        )

      )))

;; Todo: finish implementing with tests for basic defined behavior of put/get/update/delete, including error cases.
;; Todo: test ref and component attributes

(deftest batch
  (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                               '(arachne.chimera.adapter-test/test-config 0))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    ;; TODO: test batch operations, including tests for atomicity and transactionality.

    ))

;; TODO: refactor so these tests can be applied as a battery, to any adapter, not just the test adapter.
;; TODO: Write Datomic, K/V & SQL adapters, and apply these tests
