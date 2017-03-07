(ns arachne.chimera.operation
  "Utilities for constructing and interacting with core Chimera operations"
  (:require [arachne.error :refer [error deferror]]))


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