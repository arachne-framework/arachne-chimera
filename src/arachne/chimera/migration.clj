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

(defmulti operation->model
  "Apply a migration operation to an adapter's domain data model.

   Takes a config, the entity ID of an adapter, the entity ID of the migration,
   and an operation entity map.

   Dispatches based on the operation type."
  (fn [cfg adapter migration operation]
    (:chimera.migration.operation/type operation))
  :default ::unknown-operation)

(deferror ::unknown-operation
  :message "Unknown schema migration operation type `:op-type` in migration `:mig-name`"
  :explanation "Chimera was trying to build a domain model out of a set of migrations.

  However, in a migration named `:mig-name`, there was an operation with an operation type `:op-type`. Chimera does not know how to apply that type of operation to build a domain model."
  :suggestions ["Ensure that the type name `:op-type` is correct with no typos."
                "If you are a module author and need to define a new operation type, extend the `arachne.chimera.migration/apply-schema-operation` multimethod with an implementation `:op-type`."]
  :ex-data-docs {:op-type "The unknown operation type"
                 :operation "The entity map for the unknown operation"
                 :mig-name "The migration name"
                 :cfg "The current configuration"})

(defmethod operation->model ::unknown-operation
  [cfg adapter migration operation]
  (error ::unknown-schema-operation
    {:op-type (:chimera.migration.operation/type operation)
     :cfg cfg
     :mig-name (cfg/attr cfg migration :chimera.migration/name)
     :operation operation}))

(deferror ::attribute-already-exists
  :message "Cannot build domain model: attribute `:attr` already exists for type `:domain`"
  :explanation "Chimera was trying to build a domain model out of a sequence of migrations.

   One of these migrations, named `:mig-name`, contained a `create-attribute` operation, defining an attribute named `:attr` on type `:domain`. That attribute was already defined in the current model for the adapter, meaning that it must already have been defined by a previous migration.

   The adapter in question has entity ID `:adapter-eid` and Arachne ID `:adapter-aid`.

   `create-attribute` operations may only be used to define new attributes, not modify ones that might already exist."
  :suggestions ["Use a different operation if you need to modify an existing attribute."
                "Make sure there aren't two different 'chains' or branches of migrations that contain the same attribute."]
  :ex-data-docs {:attr "The name of the attribute"
                 :domain "The type to which the attribute 'belongs'"
                 :mig-name "The name of the migration"
                 :operation "The add-attribute operation entity map"
                 :adapter-eid "Entity ID of the adapter"
                 :adapter-aid "Arachne ID of the adapter"})

(defn- assert-new-attr
  "Throw an error if the attr with the given name already exists on the given adapter."
  [cfg adapter migration operation attr]
  (let [existing (cfg/q cfg '[:find ?attr .
                              :in $ ?adapter ?attr-name
                              :where
                              [?adapter :chimera.adapter/model ?attr]
                              [?attr :chimera.attribute/name ?attr-name]]
                   adapter (:chimera.attribute/name attr))]
    (when existing
      (error ::attribute-already-exists
        {:attr (:chimera.attribute/name attr)
         :domain (:chimera.attribute/domain attr)
         :mig-name (cfg/attr cfg migration :chimera.migration/name)
         :operation operation
         :adapter-eid adapter
         :adapter-aid (cfg/attr cfg adapter :arachne/id)}))))

(defn- type-exists?
  "Test if the given type name already exists in the given adapter domain model."
  [cfg adapter type-name]
  (cfg/q cfg '[:find ?type .
               :in $ ?adapter ?name
               :where
               [?adapter :chimera.adapter/model ?type]
               [?type :chimera.type/name ?name]]
    adapter type-name))

(defn- update-model-with-attr
  "Add an attribute to the domain model for the given adapter"
  [cfg adapter attr]
  (let [domain (:chimera.attribute/domain attr)]
    (cfg/with-provenance :module `update-model-with-attr
      (let [cfg' (cfg/update cfg [{:db/id adapter, :chimera.adapter/model attr}])]
        (if (type-exists? cfg' adapter domain)
          cfg'
          (cfg/update cfg' [{:db/id adapter,
                             :chimera.adapter/model {:chimera.type/name domain}}]))))))

(defmethod operation->model :chimera.operation/add-attribute
  [cfg adapter migration operation]
  (let [attr (:chimera.migration.operation.add-attribute/attr operation)]
    (assert-new-attr cfg adapter migration operation attr)
    (update-model-with-attr cfg adapter attr)))

(defn- operations
  "Given a migration eid, return a seq of its operations (as entity maps)"
  [cfg mig-eid]
  (let [e (cfg/pull cfg '[{:chimera.migration/operation
                           [:db/id {:chimera.migration.operation/next ... }]}]
            mig-eid)
        e (:chimera.migration/operation e)
        ops (butlast (tree-seq map? (comp vector :chimera.migration.operation/next) e))
        eids (map :db/id ops)]
    (map #(cfg/pull cfg '[*] %) eids)))

(defn- migration-graph
  "Build a loom graph of a migration and its dependencies"
  [cfg mig-eid]
  (let [e (cfg/pull cfg '[:db/id {:chimera.migration/parents ...}] mig-eid)
        nodes (tree-seq map? :chimera.migration/parents e)
        edges (mapcat (fn [node]
                        (map (fn [parent]
                               [(:db/id node)
                                (:db/id parent)])
                          (:chimera.migration/parents node)))
                nodes)]
    (apply loom/digraph edges)))

(defn- adapter-model
  "Given a config and an adapter eid, update the config with a domain data model
  derived from the adapter's migrations.

  This effectively transforms from a cross-time view of entity types
  (migrations) to a point-in-time view (a domain model)"
  [cfg [adapter-eid base-migrations]]
  (let [graph (->> base-migrations
                (map #(migration-graph cfg %))
                (apply loom/digraph))
        migrations (reverse (loom-alg/topsort graph))
        operations (mapcat (fn [mig]
                             (map (fn [op] [mig op]) (operations cfg mig)))
                     migrations)]
    (reduce (fn [cfg [mig op]]
              (operation->model cfg adapter-eid mig op))
      cfg operations)))

(defn ensure-migration-models
  "For every adapter in the configuration with migrations, ensure that it has a
  point-in-time data model associated with it, based on its migrations."
  [cfg]
  (let [adapters (cfg/q cfg '[:find ?a (distinct ?m)
                              :in $
                              :where
                              [?a :chimera.adapter/migrations ?m]
                              [(missing? $ ?a :chimera.adapter/model)]])]
    (reduce adapter-model cfg adapters)))

(defn add-root-migration
  "Add the root migration entity to the config"
  [cfg]
  (cfg/with-provenance :module `add-root-migration
    (cfg/update cfg
      [{:db/id (cfg/tempid)
        :chimera.migration/name :chimera/root-migration
        :chimera.migration/doc "Chimera's default root migration"}])))
