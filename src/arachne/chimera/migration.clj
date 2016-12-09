(ns arachne.chimera.migration
  (:require [clojure.spec :as s]
            [clojure.set :as set]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core :as core]
            [arachne.core.config.specs :as cfg-specs]
            [arachne.core.config :as cfg]
            [arachne.core.config.init :as script :refer [defdsl]]
            [arachne.chimera.specs :as cs]
            [loom.graph :as loom]
            [loom.alg :as loom-alg]
            [arachne.core.util :as util]))

(defn- migration-graph
  "Build a loom graph of an individual migration"
  [cfg migration-eid]
  (let [e (cfg/pull cfg '[:chimera.migration/name {:chimera.migration/parents ...}]
            migration-eid)
        nodes (tree-seq map? :chimera.migration/parents e)
        edges (mapcat (fn [node]
                        (map (fn [parent]
                               [(:chimera.migration/name node)
                                (:chimera.migration/name parent)])
                          (:chimera.migration/parents node)))
                nodes)]
    (apply loom/digraph edges)))

(defn- operations
  "Given a migration, return a seq of its operations (as entity maps)"
  [cfg migration]
  (let [e (cfg/pull cfg '[{:chimera.migration/operation
                           [:db/id {:chimera.migration.operation/next ... }]}]
            [:chimera.migration/name migration])
        e (:chimera.migration/operation e)
        ops (butlast (tree-seq map? (comp vector :chimera.migration.operation/next) e))
        eids (map :db/id ops)]
    (map #(cfg/pull cfg '[*] %) eids)))


(s/def :arachne.chimera.migration.data-model/types
  (s/map-of :chimera.type/name ::cs/type-txmap))

(s/def :arachne.chimera.migration.data-model/attrs
  (s/map-of :chimera.attribute/name ::cs/attribute-txmap))

(s/def ::data-model (s/keys :req-un [:arachne.chimera.migration.data-model/types
                                     :arachne.chimera.migration.data-model/attrs]))

(s/fdef apply-schema-operation
  :args (s/cat :domain ::data-model
               :operation ::cs/operation-txmap
               :migration-name :chimera.migration/name)
  :ret ::data-model)

(defmulti apply-schema-operation
  "Given a domain model data structure and the entity map of a schema operation,
  return an updated domain model."
  (fn [model operation migration-name]
    (:chimera.migration.operation/type operation))
  :default ::unknown-operation)

(deferror ::unknown-schema-operation
  :message "Unknown schema migration operation type `:op-type` in migration `:mig-name`"
  :explanation "Chimera was trying to build a domain model out of a set of migrations.

  However, in a migration named `:mig-name`, there was an operation with an operation type `:op-type`. Chimera does not know how to apply that type of operation to build a domain model."
  :suggestions ["Ensure that the type name `:op-type` is correct with no typos."
                "If you are a module author and need to define a new operation type, extend the `arachne.chimera.migration/apply-schema-operation` multimethod with an implementation `:op-type`."]
  :ex-data-docs {:op-type "The unknown operation type"
                 :operation "The entity map for the unknown operation"
                 :mig-name "The migration name"
                 :migration "The migration entity map"})

(defmethod apply-schema-operation ::unknown-schema-operation
  [model operation migration-name]
  (error ::unknown-schema-operation
    {:op-type (:chimera.migration.operation/type operation)
     :mig-name migration-name
     :operation operation}))

(defmethod apply-schema-operation :chimera.operation/extend-type
  [model operation migration-name]
  (let [sub (:chimera.migration.operation.extend-type/subtype operation)
        sup (:chimera.migration.operation.extend-type/supertype operation)]
    (-> model
      (assoc-in [:types sub :chimera.type/name] sub)
      (update-in [:types sub :chimera.type/supertypes] (fnil conj #{}) sup)
      (update-in [:types sup] merge {:chimera.type/name sup}))))


(deferror ::attribute-already-exists
  :message "Cannot build domain model: attribute `:attr` already exists"
  :explanation "Chimera was trying to build a domain model out of a sequence of migrations.

   One of these migrations, named `:mig-name`, contained a `create-attribute` operation, defining an attribute named `:attr`. That attribute was already defined in the current model.

   `create-attribute` operations may only be used to define new attributes, not modify ones that might already exist."
  :suggestions ["Use a different operation if you need to modify an existing attribute."
                "Make sure there aren't two different 'chains' or branches of migrations that contain the same attribute."]
  :ex-data-docs {:attr "The name of the attribute"
                 :mig-name "The name of the migration"
                 :operation "The add-attribute operation entity map"})

(defmethod apply-schema-operation :chimera.operation/add-attribute
  [model operation migration-name]
  (let [attr (-> operation
               :chimera.migration.operation.add-attribute/attr
               :chimera.attribute/name)]
    (when (get-in model [:attrs attr])
      (error ::attribute-already-exists
        {:attr attr
         :mig-name migration-name
         :operation operation}))
    (assoc-in model [:attrs attr]
      (:chimera.migration.operation.add-attribute/attr operation))))

(defn- apply-migration
  "Given a domain model data structure and a migration, apply all the operations
  in the migration to the data model."
  [cfg model migration-name]
  (reduce #(apply-schema-operation %1 %2 migration-name)
    model
    (operations cfg migration-name)))

(s/fdef rollup
  :args (s/cat :config ::cfg-specs/config
               :migrations (s/coll-of int?))
  :ret ::data-model)

(defn rollup
  "Given a config and a set of migration eids, returns a domain model data structure.

  This effectively transforms from a cross-time view of entity types
  (migrations) to a point-in-time view (a domain model)"
  [cfg migrations]
  (let [graph (apply loom/digraph (map #(migration-graph cfg %) migrations))
        migs (reverse (loom-alg/topsort graph))
        model {:types {}, :attrs {}}]
    (reduce #(apply-migration cfg %1 %2) model migs)))


(defn add-root-migration
  "Add the root migration entity to the config"
  [cfg]
  (cfg/with-provenance :module `add-root-migration
    (cfg/update cfg
      [{:db/id (cfg/tempid)
        :chimera.migration/name :chimera/root-migration
        :chimera.migration/doc "Chimera's default root migration"}])))

;; TODO: Build a domain model from a config schema

(comment

  (def cfg (core/build-config '[:org.arachne-framework/arachne-chimera]
                '(do (require '[arachne.core.dsl :as a])
                     (require '[arachne.chimera.dsl :as c])

                     (a/runtime :test/rt [(a/component :test/a {} 'clojure.core/hash-map)])

                     (c/migration :test/m1
                       "test migration"
                       []

                       (c/attr :test/attr-a :test/Type :string :min 1)
                       (c/attr :test/attr-b :test/Subtype :ref :test/Type :min 1 :max 1)

                       (c/extend-type :test/Type :test/Subtype))

                     (c/migration :test/t1
                       "test migration 2"
                       []

                       (c/attr :test/attrx :test/Type :string :min 1)

                       (c/extend-type :test/Type :test/Subtype))

                     (c/migration :test/m2
                       "test migration"
                       [:test/m1]

                       (c/attr :test/attr2 :test/Type :string :min 1))



                     (a/transact
                       [{:arachne/id :test/adapter
                         :arachne.component/constructor :clojure.core/hash-map
                         :chimera.adapter/capabilities [{:chimera.adapter.capability/operation :chimera.operation/read
                                                         :chimera.adapter.capability/idempotent true
                                                         :chimera.adapter.capability/transactional true}]
                         :chimera.adapter/migrations [{:chimera.migration/name :test/m2}
                                                      {:chimera.migration/name :test/t1}]}])
                     )
             true))




  (pprint
    (rollup cfg [[:chimera.migration/name :test/m2]]))



  (require '[loom.io :as gv])
  (loom/edges (migration-graph cfg :test/m2))

  (use 'clojure.pprint)

  (pprint
    (operations cfg :test/m2))

  (pprint
    (rollup cfg [:test/m2 :test/t1]))

  )