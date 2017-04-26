(ns arachne.chimera.migration
  (:require [clojure.spec :as s]
            [clojure.set :as set]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core :as core]
            [arachne.core.config.specs :as cfg-specs]
            [arachne.core.config :as cfg]
            [arachne.core.config.script :as script :refer [defdsl]]
            [arachne.chimera.specs :as cs]
            [loom.graph :as loom]
            [loom.alg :as loom-alg]
            [arachne.core.util :as util]
            [clojure.walk :as w]))

(defmulti operation->model
  "Apply a migration operation to an adapter's domain data model.

   Takes a config, the entity ID of an adapter, the entity ID of the migration,
   and an operation entity map.

   Dispatches based on the operation type."
  (fn [cfg adapter migration operation]
    (:chimera.migration.operation/type operation))
  :default ::unknown-operation)

(deferror ::unknown-schema-operation
  :message "Unknown schema migration operation type `:op-type` in migration `:mig-name`"
  :explanation "Chimera was trying to build a domain model out of a set of migrations.

  However, in a migration named `:mig-name`, there was an operation with an operation type `:op-type`. Chimera does not know how to apply that type of operation to build a domain model."
  :suggestions ["Ensure that the type name `:op-type` is correct with no typos."
                "If you are a module author and need to define a new operation type, extend the `arachne.chimera.migration/operation->model` multimethod with an implementation `:op-type`."]
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

(defn operations
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

(defn migrations
  "Return a sequence of all the dependent migration eids in an adapter, in
  dependency order"
  [cfg adapter-eid]
  (->> (cfg/pull cfg '[:chimera.adapter/migrations] adapter-eid)
    :chimera.adapter/migrations
    (map :db/id)
    (map #(migration-graph cfg %))
    (apply loom/digraph)
    (loom-alg/topsort)
    (reverse)))

(deferror ::unknown-canonical-operation
  :message "Unknown schema migration operation type `:op-type` in migration `:mig-name`"
  :explanation "Chimera was trying to build the MD5 signature of a Chimera migration, to ensure its integrity and that it hasn't changed since the first time it was applied to a database..

  However, in a migration named `:mig-name`, there was an operation with an operation type `:op-type`. Chimera does not know how to obtain a 'canonical' view of this operation type to obtain a stable MD5 signature."
  :suggestions ["Ensure that the type name `:op-type` is correct with no typos."
                "If you are a module author and need to define a new operation type, extend the `arachne.chimera.migration/canonical-operation` multimethod with an implementation `:op-type`."]
  :ex-data-docs {:op-type "The unknown operation type"
                 :operation "The entity map for the unknown operation"
                 :mig-name "The migration name"
                 :cfg "The current configuration"})

(defmulti canonical-operation
  "Return the stable, official view of a migration operation, dispatching
   based on the migration operation type."
  (fn [cfg migration operation]
    (:chimera.migration.operation/type operation))
  :default ::unknown-operation)

(defmethod canonical-operation ::unknown-operation
  [cfg migration operation]
  (error ::unknown-canonical-operation
    {:op-type (:chimera.migration.operation/type operation)
     :cfg cfg
     :mig-name (:chimera.migration/name migration)
     :operation operation}))

(defmethod canonical-operation :chimera.operation/add-attribute
  [cfg migration operation]
  operation)

(defn canonical-migration
  "Return the stable, offical view of a migration, as passed to the migrate operation
  and as used to calculate the migration's signature."
  [cfg migration-eid]
  (let [m (cfg/pull cfg '[:chimera.migration/name
                          :chimera.migration/operation
                          {:chimera.migration/parents [:chimera.migration/name]}] migration-eid)
        m (w/prewalk (fn [form]
                       (if (:chimera.migration.operation/type form)
                         (canonical-operation cfg m form)
                         form))
            m)]
    (w/prewalk (fn [form]
                 (if (map? form)
                   (dissoc form :db/id)
                   form))
      m)))

(defn- adapter-model
  "Given a config and an adapter eid, update the config with a domain data model
  derived from the adapter's migrations.

  This effectively transforms from a cross-time view of entity types
  (migrations) to a point-in-time view (a domain model)"
  [cfg adapter-eid]
  (let [migs (migrations cfg adapter-eid)
        operations (mapcat (fn [mig]
                             (map (fn [op] [mig op]) (operations cfg mig)))
                     migs)]
    (reduce (fn [cfg [mig op]]
              (operation->model cfg adapter-eid mig op))
      cfg operations)))

(defn ensure-migration-models
  "For every adapter in the configuration with migrations, ensure that it has a
  point-in-time data model associated with it, based on its migrations."
  [cfg]
  (let [adapters (cfg/q cfg '[:find [?a ...]
                              :in $
                              :where
                              [?a :chimera.adapter/migrations _]
                              [(missing? $ ?a :chimera.adapter/model)]])]
    (reduce adapter-model cfg adapters)))

(defn add-root-migration
  "Add the root migration entity to the config. The root migration is the start of the migration
   chain for all adapters."
  [cfg]
  (cfg/with-provenance :module `add-root-migration
    (cfg/update cfg
      [{:db/id (cfg/tempid)
        :chimera.migration/name :chimera/root-migration
        :chimera.migration/doc "Chimera's default root migration"}])))

(deferror ::invalid-signature
  :message "Invalid migration checksum for migration `:name` in adapter `:adapter-eid` (Arachne ID: `:adapter-aid`)"
  :explanation "To ensure the integrity of existing databases, Chimera generates a signature for
                each migration in the configuration. Once a migration is applied to a database, it
                cannot be modified and re-applied to the same database. The migration process
                ensures that either a migration is entirely new (in which case it is applied), or
                that its checksum exactly matches the existing migration (in which case it is
                guaranteed that all its changes have already been applied, and it is a no-op).

                In this case, the migration named `:name` appears to be different than it was the
                first time it was applied to this database, at `:original-time-str`. The
                original MD5 signature was `:original`, the new one was `:new`.

                This signature is obtained by hashing the name and parents of a migration, and the
                full entity maps for all of a migration's operations. Any change, however minor, to
                any of the operations in a migration will cause its checksum to change."
  :suggestions ["Revert the migration to its original form, so it will have the same signature as
                 the version currently in the database. If you really do need to change something
                 about your database's schema, use a new migration instead."
                "If you don't care about the data in your existing database, delete it and start
                 with a new database. Without a previous migration to compare to, the new form of
                 the migration should apply cleanly."]
  :ex-data-docs {:name "Name of the migration"
                 :adapter-eid "Entity ID of the adapter."
                 :adapter-aid "Arachne ID of the adapter"
                 :original-time "Date of the original migration"
                 :original-time-str "Date string of the original"
                 :original "The original MD5 signature"
                 :new "The new MD5 signature"})
