(ns arachne.chimera.migration-test
  (:require [clojure.test :refer :all]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [com.stuartsierra.component :as component]))

(deftest migration-rollups
  (let [cfg (core/build-config '[:org.arachne-framework/arachne-chimera]
              '(do (require '[arachne.core.dsl :as a])
                   (require '[arachne.chimera.dsl :as c])

                   (a/runtime :test/rt [(a/component :test/a {} 'clojure.core/hash-map)])

                   (c/migration :test/m1
                     "test migration"
                     []

                     (c/attr :test/a :test/Type :string :min 1)
                     (c/attr :test/b :test/Subtype :ref :test/Type :min 1 :max 1)

                     (c/extend-type :test/Type :test/Subtype))

                   (c/migration :test/m3
                     "test migration"
                     []

                     (c/attr :test/c :test/Type :string :min 1))

                   (c/migration :test/m2
                     "test migration"
                     [:test/m1]

                     (c/attr :test/d :test/Type :string :min 1))

                   (a/transact
                     [{:arachne/id :test/adapter
                       :chimera.adapter/type :test/adapter-type
                       :arachne.component/constructor :clojure.core/hash-map
                       :chimera.adapter/capabilities [{:chimera.adapter.capability/operation :chimera.operation/get
                                                       :chimera.adapter.capability/idempotent true
                                                       :chimera.adapter.capability/atomic true
                                                       :chimera.adapter.capability/transactional true}]
                       :chimera.adapter/migrations [{:chimera.migration/name :test/m2}
                                                    {:chimera.migration/name :test/m3}]}])
                   ))]

    (is (cfg/q cfg '[:find ?a .
                     :where
                     [?a :chimera.adapter/model ?me]
                     [?me :chimera.type/name ?subtype]
                     [?me :chimera.type/supertypes ?supertype]
                     [(= ?supertype :test/Type)]
                     [(= ?subtype :test/Subtype)]
                     [?attr-a :chimera.attribute/domain ?supertype]
                     [?attr-a :chimera.attribute/range :chimera.primitive/string]
                     [?attr-a :chimera.attribute/min-cardinality 1]
                     [?attr-b :chimera.attribute/domain ?subtype]
                     [?attr-b :chimera.attribute/range :test/Type]
                     [?attr-x :chimera.attribute/domain :test/Type]
                     [?attr-x :chimera.attribute/name :test/d]])))

    )
