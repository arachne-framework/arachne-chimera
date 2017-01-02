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
            [com.stuartsierra.component :as component]
            [arachne.chimera.test-adapter :as ta])
  (:import [arachne ArachneException]))

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

  (ta/test-adapter :test/adapter :test/m2)

  (c/runtime :test/rt [:test/adapter])

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

#_(deftest basic-adapter
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
