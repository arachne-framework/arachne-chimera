(ns arachne.chimera.migration-test
  (:require [clojure.test :refer :all]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [com.stuartsierra.component :as component]
            [arachne.core.dsl :as a]
            [arachne.chimera.dsl :as c]
            ))

(defn migration-rollups-cfg
  []
  (a/id :test/rt (a/runtime [(a/component 'clojure.core/hash-map)]))

  (c/migration :test/m1
    "test migration"
    []

    (c/attr :test/a :test/TypeA :string :min 1)
    (c/attr :test/b :test/TypeB :ref :test/TypeA :min 1 :max 1))

  (c/migration :test/m3
    "test migration"
    []

    (c/attr :test/c :test/TypeA :string :min 1))

  (c/migration :test/m2
    "test migration"
    [:test/m1]

    (c/attr :test/d :test/TypeA :string :min 1))

  (a/transact
    [{:arachne/id :test/adapter
      :arachne.component/constructor :clojure.core/hash-map
      :chimera.adapter/capabilities [{:chimera.adapter.capability/operation :chimera.operation/get
                                      :chimera.adapter.capability/atomic true}]
      :chimera.adapter/dispatches [{:chimera.adapter.dispatch/index 0
                                    :chimera.adapter.dispatch/pattern "[_ _]"
                                    :chimera.adapter.dispatch/impl :clojure.core/identity}]
      :chimera.adapter/migrations [{:chimera.migration/name :test/m2}
                                   {:chimera.migration/name :test/m3}]}])
  )


(deftest migration-rollups
  (let [cfg (core/build-config '[:org.arachne-framework/arachne-chimera]
              `(migration-rollups-cfg))]
    (is (cfg/q cfg '[:find ?a .
                     :where
                     [?a :chimera.adapter/model ?type-a]
                     [?a :chimera.adapter/model ?type-b]
                     [?type-a :chimera.type/name :test/TypeA]
                     [?type-b :chimera.type/name :test/TypeB]

                     [?a :chimera.adapter/model ?attr-a]
                     [?attr-a :chimera.attribute/domain :test/TypeA]
                     [?attr-a :chimera.attribute/range :chimera.primitive/string]
                     [?attr-a :chimera.attribute/min-cardinality 1]

                     [?a :chimera.adapter/model ?attr-b]
                     [?attr-b :chimera.attribute/domain :test/TypeB]
                     [?attr-b :chimera.attribute/range :test/TypeA]

                     [?a :chimera.adapter/model ?attr-x]
                     [?attr-x :chimera.attribute/domain :test/TypeA]
                     [?attr-x :chimera.attribute/name :test/d]]))))