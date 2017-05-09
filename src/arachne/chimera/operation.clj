(ns arachne.chimera.operation
  "Utilities for constructing and interacting with core Chimera operations"
  (:require [arachne.core.config :as cfg]
            [arachne.chimera.specs]
            [arachne.error :refer [error deferror]]
            [clojure.spec.alpha :as s]))

(defn- context-matches-return
  "Spec function to assert that, when a function is passed a :context, its
   return value is also a context."
  [f]
  (if (nil? (:context f))
    (nil? (:ret f))
    (some? (:ret f))))

(s/def :chimera.operation/context some?)

(s/def :chimera.operation/initialize-migrations
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/initialize-migrations}
                        :payload #{true})
           :ret boolean?))

(s/def :chimera.operation.migration/signature string?)
(s/def :chimera.operation.migration/name qualified-keyword?)
(s/def :chimera.operation.migration/operations
  (s/coll-of map?))


(s/def :chimera.operation/migrate
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/migrate}
                        :payload (s/keys :req-un [:chimera.operation.migration/signature
                                                  :chimera.operation.migraiton/name
                                                  :chimera.operation.migration/operations]))
           :ret nil?))

(s/def :chimera.operation/add-attribute
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/add-attribute}
                        :payload (s/keys :req [:chimera.operation.add-attribute/attr])
                        :context :chimera.operation/context)
           :ret :chimera.operation/context))

(s/def :chimera.operation/put
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/put}
                        :payload :chimera/entity-map
                        :context (s/? :chimera.operation/context))
           :fn context-matches-return
           :ret (s/or :without-context nil?
                      :with-context :chimera.operation/context)))

(s/def :chimera.operation/get
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/get}
                        :payload :chimera/lookup)
           :ret (s/or :no-result nil?
                      :result :chimera/entity-map-with-components)))

(s/def :chimera.operation/delete-entity
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/delete-entity}
                        :payload :chimera/lookup
                        :context (s/? :chimera.operation/context))
           :fn context-matches-return
           :ret (s/or :without-context nil?
                      :with-context :chimera.operation/context)))

(s/def :chimera.operation/delete
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/delete}
                        :payload (s/and vector?
                                   (s/cat :entity :chimera/lookup
                                        :attribute :chimera.attribute/name
                                        :value (s/? (s/or :primitive :chimera/primitive
                                                           :ref :chimera/lookup))))
                   :context (s/? :chimera.operation/context))
           :fn context-matches-return
           :ret (s/or :without-context nil?
                      :with-context :chimera.operation/context)))

(s/def :chimera.operation/batch
  (s/fspec :args (s/cat :adapter :chimera/adapter
                        :op #{:chimera.operation/batch}
                        :payload (s/coll-of
                                   (s/tuple :chimera/operation-type any?)
                                   :min-count 1))
           :ret nil?))


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
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? true
    :arachne/doc "Add an attribute definition to the database."}
   {:chimera.operation/type :chimera.operation/get
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? true
    :arachne/doc "Retrieve an entity from the database."}
   {:chimera.operation/type :chimera.operation/put
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? false
    :arachne/doc "Add or update an entity in the database."}
   {:chimera.operation/type :chimera.operation/delete-entity
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? false
    :arachne/doc "Delete an entity from the database. The entity must have previously existed."}
   {:chimera.operation/type :chimera.operation/delete
    :chimera.operation/batchable? true
    :chimera.operation/idempotent? true
    :arachne/doc "Remove an attribute from an entity. If a value is specified removes only attributes with that value, otherwise removes all values of the attribute."}
   {:chimera.operation/type :chimera.operation/batch
    :chimera.operation/batchable? false
    :chimera.operation/idempotent? false
    :arachne/doc "Execute a set of operations, transactionally if possible."}])

(defn add-operations
  "Add operation definitions to the given config"
  [cfg]
  (cfg/with-provenance :module `add-operations
     (cfg/update cfg operations)))

(deferror ::entity-does-not-exist
          :message "Entity `:lookup` does not exist in adapter `:adapter-eid` (Arachne ID `:adapter-aid`)"
          :explanation "Chimera attempted to perform a `:op`, operation containing the key `:lookup`. However, an entity with that lookup does not already exist in the target DB.

           The operation requires that the the specified entity exist in the data source."
          :suggestions ["Ensure that the entity you are attempting to modify exists in the given adapter."]
          :ex-data-docs {:lookup "The lookup key"
                         :op "The operation"
                         :adapter-eid "Adapter entity ID"
                         :adapter-aid "Adapter Arachne ID"})

(deferror ::no-key-specified
          :message "The requested operation requires a key attribute."
          :explanation "Chimera attempted to perform an operation which takes an entity map. Furthermore, it requires that the supplied entity map contain at least one 'key' attribute, so that the created or updated entity can be uniquely identified.

          The adapater for this operation was `:adapter-eid` (Arachne ID: `:adapter-aid`)

          However, the given entity map did not proved any attributes identified as 'key' attributes in the Chimera schema.

          The attributes provided in the entity map were:

              :provided-attrs-str

          Attributes identified as keys in the given adapter are:

              :key-attrs-str"
          :suggestions ["Make sure the entity map has a key attribute"]
          :ex-data-docs {:adapter-eid "Adapter entity ID"
                         :adapter-aid "Adapter Arachne ID"
                         :provided-attrs "Entity map attrs"
                         :provided-attrs-str "Entity map attrs (formatted)"
                         :key-attrs "Key attrs in the adapter schema"
                         :key-attrs-str "Key attrs (formatted)"})


(deferror ::unexpected-cardinality-one
          :message "Value for attribute `:attribute` must be a set"
          :explanation "Chimera attempted to perform a `:op` operation. The supplied entity map contained the key `:attribute`, with a value of `:value`. However, this value was given as a single value, rather than as a set of values.

           Because `:attribute` is a cardinality-many attribute (that is, it may have more than one value), its values should always be represented as a set when passing an entity map to a Chimera operation."
          :suggestions ["Use a set instead of a single value. The set may contain one element if desired."]
          :ex-data-docs {:attribute "the attribute"
                         :value "the value provided"
                         :op "The operation"
                         :adapter-eid "Adapter entity ID"
                         :adapter-aid "Adapter Arachne ID"})

(deferror ::unexpected-cardinality-many
          :message "Value for attribute `:attribute` must be a single value"
          :explanation "Chimera attempted to perform a `:op` operation. The supplied entity map contained the key `:attribute`, with a value of `:value`. However, this value was given as a set, rather than as a single value.

           Because `:attribute` is a cardinality-one attribute, (that is, it may have at most one value), the value should always be represented as a single value when passing an entity map to a Chimera operation."
          :suggestions ["Use a single value instead of a set."]
          :ex-data-docs {:attribute "the attribute"
                         :value "the value provided"
                         :op "The operation"
                         :adapter-eid "Adapter entity ID"
                         :adapter-aid "Adapter Arachne ID"})

(deferror ::cannot-delete-key-attr
  :message "Cannot delete key attribute `:attribute`"
  :explanation "Chimera attempted to perform a `:chimera.operation/delete` operation, to remove the value of the `:attribute` value from an entity identified by `:lookup`.

  However, the `:attribute` attribute is a 'key attribute', meaning that it is used as the primary identifier of an entity. Key attributes cannot be deleted, because that would make it impossible to reference the entity in the system."
  :suggestions ["Delete the entire entity using the `:chimera.operation/delete-entity` operation"
                "Delete other attributes of the entity"]
  :ex-data-docs {:attribute "the key attribute"
                 :lookup "The entity targeted by the delte"
                 :adapter-eid "Adapter entity ID"
                 :adapter-aid "Adapter Arachne ID"})
