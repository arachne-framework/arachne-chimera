(ns arachne.chimera.dsl-test
  (:require [clojure.test :refer :all]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [com.stuartsierra.component :as component]))

(deftest basic-dsl
  (let [cfg (core/build-config '[:org.arachne-framework/arachne-chimera]
              '(do (require '[arachne.core.dsl :as a])
                   (require '[arachne.chimera.dsl :as c])

                   (a/runtime :test/rt [(a/component :test/a {} 'clojure.core/hash-map)])

                   (c/migration :test/m1
                     "test migration"
                     []

                     (c/attr :test/attr :test/TypeA :string :min 1)
                     (c/attr :test/attr :test/TypeB :ref :test/TypeA :min 1 :max 1)))
              false)]

    (is (= 2 (count (cfg/q cfg '[:find ?m
                                 :where [?m :chimera.migration/name _]]))))

    (is (= 2 (count (cfg/q cfg
                      '[:find ?op
                        :where
                        [?op :chimera.migration.operation/type
                         :chimera.operation/add-attribute]]))))))

