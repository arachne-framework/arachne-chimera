(ns arachne.chimera.dsl-test
  (:require [clojure.test :refer :all]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [com.stuartsierra.component :as component]
            [arachne.core.dsl :as a]
            [arachne.chimera.dsl :as c]))

[(defn basic-dsl-cfg
   []

   (a/id :test/rt (a/runtime [(a/component 'clojure.core/hash-map)]))

   (c/migration :test/m1
     "test migration"
     []

     (c/attr :test/attr :test/TypeA :string :min 1)
     (c/attr :test/attr :test/TypeB :ref :test/TypeA :min 1 :max 1)))]

(deftest basic-dsl
  (let [cfg (core/build-config '[:org.arachne-framework/arachne-chimera]
              `(basic-dsl-cfg)
              false)]

    (is (= 2 (count (cfg/q cfg '[:find ?m
                                 :where [?m :chimera.migration/name _]]))))

    (is (= 2 (count (cfg/q cfg
                      '[:find ?op
                        :where
                        [?op :chimera.migration.operation/type
                         :chimera.operation/add-attribute]]))))))

