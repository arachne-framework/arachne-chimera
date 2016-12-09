(ns arachne.chimera.schema
  (:require [arachne.core.config.model :as m]))


(def schema
  "Schema for the Chimera module"
  (concat

    (m/type :chimera/Adapter [:arachne/Component]
      "An adapter component"
      (m/attr :chimera.adapter/type :one :keyword
        "Identifier for the type of the adapter. Adapters of the same type should have the same constructor.")
      (m/attr :chimera.adapter/model :many :component :chimera/DataModelElement
        "Data model elements that are part of the concrete data model for this Adapter.")
      (m/attr :chimera.adapter/migrations :many :chimera/Migration
        "Migrations that are a part of this Adapter (as a cross-time model)")
      (m/attr :chimera.adapter/capabilities :component :one-or-more :arachne.adapter/Capability
        "Operations that this Adapter supports"))

    (m/type :chimera.adapter/Capability []
      "Details about the level of support an adapter gives for a particular operation"
      (m/attr :chimera.adapter.capability/operation :one :keyword
        "The operation that this capability describes.")
      (m/attr :chimera.adapter.capability/atomic :one :boolean
        "Is the operation itself atomic? That is, is it guaranteed to succeed or
        fail as a unit, and leave the database untouched if it fails?")
      (m/attr :chimera.adapter.capability/transactional :one :boolean
        "Is the operation transactional with respect to other operations (in the
        same batch?)")
      (m/attr :chimera.adapter.capability/idempotent :one :boolean
        "Is the operation idempotent? That is, does applying it to the database
        multiple times yield the same result as applying it once?"))

    ;; Static Entity Model
    (m/type :chimera/DataModelElement []
      "The abstract type for data model entities (types and attributes)")

    (m/type :chimera/Type [:chimera/DataModelElement]
      "An entity type"
      (m/attr :chimera.type/name :one :keyword
        "The unique name of the type.")
      (m/attr :chimera.type/supertypes :many :keyword
        "The names of the supertypes of this type"))

    (m/type :chimera/Attribute [:chimera/DataModelElement]
      "An attribute."
      (m/attr :chimera.attribute/name :one :keyword
        "The unique name of the attribute.")
      (m/attr :chimera.attribute/min-cardinality :one :long
        "The minimum required number of values for this attribute.")
      (m/attr :chimera.attribute/max-cardinality :one-or-none :long
        "The maximum allowed number of values for this attribute.")
      (m/attr :chimera.attribute/domain :one :keyword
        "The name of the type to which this attribute can be (validly) applied.")

      (m/attr :chimera.attribute/range :one :keyword
        "The value type for this attribute. May be a primitive type or the keyword name of an entity type.")

      (m/attr :chimera.attribute/component :one-or-none :boolean
        "If true, indicates that a ref value is a boolean.")

      (m/attr :chimera.attribute/key :one-or-none :boolean
        "Can the value of this attribute be used to uniquely identify entities?")
      (m/attr :chimera.attribute/indexed :one-or-none :boolean
        "Is efficient lookup for this attribute expected?"))


    ;; Migration Model
    (m/type :chimera/Migration []
      "A migration represents a logical set of changes to a database."
      (m/attr :chimera.migration/name :one :keyword :identity
        "Unique name for a migration")
      (m/attr :chimera.migration/doc :one :string
        "Documentation for the migration")
      (m/attr :chimera.migration/parents :many :chimera/Migration
        "Migrations that must be in place before this one can be applied")
      (m/attr :chimera.migration/operation :one-or-none :component :chimera.migration/Operation
        "Operations which are a part of this migration. Pointer to the head of a linked list of operations."))

    (m/type :chimera.migration/Operation []
      "An individual operation that is a part of a migration."
      (m/attr :chimera.migration.operation/next :one-or-none :component :chimera.migration/Operation
        "The next operation in this sequence")
      (m/attr :chimera.migration.operation/type :one :keyword
        "The type of the operation"))

    (m/type :chimera.migration.operation/AddAttribute []
      "Encoding of the data required for an AddAttribute operation"
      (m/attr :chimera.migration.operation.add-attribute/attr :one :component :chimera/Attribute
        "Attribute entity to be added to the model"))

    (m/type :chimera.migration.operation/ExtendType []
      "Encoding of the data required for an ExtendType operation"
      (m/attr :chimera.migration.operation.extend-type/supertype :one :keyword
        "The supertype of the type being extended")
      (m/attr :chimera.migration.operation.extend-type/subtype :one :keyword
        "The subtype that is being extended"))))