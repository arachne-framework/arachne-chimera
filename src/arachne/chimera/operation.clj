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

