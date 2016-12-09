(ns arachne.chimera.adapter.config-test
  (:require [clojure.test :refer :all]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [com.stuartsierra.component :as component]
            [arachne.chimera :as ch]))

(defn- test-config []
  (core/build-config '[:org.arachne-framework/arachne-chimera]
    '(do (require '[arachne.core.dsl :as a])
         (require '[arachne.chimera.dsl :as c])

         (c/config-adapter :my.app/config-adapter)

         (a/runtime :test/rt [:my.app/config-adapter])

         (c/migration :test/m1
           "test migration"
           []

           (c/attr :test/attr-a :test/Type :string :min 1)
           (c/attr :test/attr-b :test/Subtype :ref :test/Type :min 1 :max 1)

           (c/extend-type :test/Type :test/Subtype))
         ) true))

(deftest get-operation
  (let [cfg (test-config)
        rt (component/start (rt/init cfg [:arachne/id :test/rt]))
        a (rt/lookup rt [:arachne/id :my.app/config-adapter])]

    (is (ch/adapter? a))

    (is (= [] (ch/operate a :chimera.operation/get
                [:arachne/id :test/rt])))
    ))

