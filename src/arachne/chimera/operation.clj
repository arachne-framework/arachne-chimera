(ns arachne.chimera.operation
  "Utilities for constructing and interacting with core Chimera operations"
  (:require [arachne.core.config :as cfg]
            [arachne.chimera.specs]
            [arachne.error :refer [error deferror]]
            [clojure.spec :as s]))

(s/def :chimera.operation/initialize-migrations #{true})

(s/def :chimera.operation/migrate
  (s/cat :signature string?
         :migration map?))

(s/def :chimera.operation/put :chimera/entity-map)
(s/def :chimera.operation/get :chimera/lookup)
(s/def :chimera.operation/update :chimera/entity-map)
(s/def :chimera.operation/delete :chimera/lookup)

(s/def :chimera.operation/batch (s/coll-of
                                 (s/tuple :chimera/operation-type any?)
                                 :min-count 1))


(def operations
  [{:chimera.operation/type :chimera.operation/initialize-migrations
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? true
    :arachne/doc "Initialize a database to support Chimera's migration model. This usually involves installing whatever schema is necessary to track migrations. For databases without a schema, this may be a no-op."}
   {:chimera.operation/type :chimera.operation/migrate
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? true
    :arachne/doc "Apply a migration to a database."}
   {:chimera.operation/type :chimera.operation/add-attribute
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? true
    :arachne/doc "Add an attribute definition to the database."}
   {:chimera.operation/type :chimera.operation/get
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? true
    :arachne/doc "Retrieve an entity from the database."}
   {:chimera.operation/type :chimera.operation/put
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? false
    :arachne/doc "Add a new entity to the database. The entity must not previously exist (as determined by any key attributes)."}
   {:chimera.operation/type :chimera.operation/update
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? true
    :arachne/doc "Update an entity in the database. The entity must already exist."}
   {:chimera.operation/type :chimera.operation/delete
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? false
    :arachne/doc "Delete an entity from the database. The entity must have previously existed."}
   {:chimera.operation/type :chimera.operation/batch
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? false
    :arachne/doc "Execute a set of operations, transactionally if possible."}])

(defn add-operations
  "Add operation definitions to the given config"
  [cfg]
  (cfg/with-provenance :module `add-operations
     (cfg/update cfg operations)))

(deferror ::entity-already-exists
  :message "Entity `:lookup` already exists in adapter `:adapter-eid` (Arachne ID `:adapter-aid`)"
  :explanation "Chimera attempted to `put` a value, using the key `:lookup`. However, an entity with that lookup already exists in the target DB.

  Chimera does not support \"upsert\" for `put` operations; `put` is intended only for new entities."
  :suggestions ["Use an `update` operation to update an existing entity."
                "Determine a new key attribute value for the entity that doesn't already exist."]
  :ex-data-docs {:lookup "The lookup key"
                 :adapter-eid "Adapter entity ID"
                 :adapter-aid "Adapter Arachne ID"})

(deferror ::entity-does-not-exist
          :message "Entity `:lookup` does not exist in adapter `:adapter-eid` (Arachne ID `:adapter-aid`)"
          :explanation "Chimera attempted to perform `:op` on an entity, using the key `:lookup`. However, an entity with that lookup does not exist in the target DB.

           The `:op` operation requires that the the specified entity exist in the data source."
          :suggestions ["Ensure that the entity you are attempting to modify exists in the given adapter."]
          :ex-data-docs {:lookup "The lookup key"
                         :op "The operation"
                         :adapter-eid "Adapter entity ID"
                         :adapter-aid "Adapter Arachne ID"})

(deferror ::no-key-specified
          :message "The requested operation `:op` requires a key attribute."
          :explanation "Chimera attempted to perform a `:op` operation, which takes an entity map. Furthermore, it requires that the supplied entity map contain at least one 'key' attribute, so that the created or updated entity can be uniquely identified.

          The adapater for this operation was `:adapter-eid` (Arachne ID: `:adapter-aid`)

          However, the given entity map did not proved any attributes identified as 'key' attributes in the Chimera schema.

          The attributes provided in the entity map were:

              :provided-attrs-str

          Attributes identified as keys in the given adapter are:

              :key-attrs-str"
          :suggestions ["Make sure the entity map has a key attribute"]
          :ex-data-docs {:op "The operation type"
                         :adapter-eid "Adapter entity ID"
                         :adapter-aid "Adapter Arachne ID"
                         :provided-attrs "Entity map attrs"
                         :provided-attrs-str "Entity map attrs (formatted)"
                         :key-attrs "Key attrs in the adapter schema"
                         :key-attrs-str "Key attrs (formatted)"})