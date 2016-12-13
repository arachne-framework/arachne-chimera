(ns arachne.chimera
  (:require [clojure.spec :as s]
            [arachne.error :as e :refer [error deferror]]
            [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.chimera.specs]
            [arachne.chimera.schema :as schema]
            [arachne.chimera.migration :as migrations]
            [arachne.chimera.adapter :as adapter]))

(defn schema
  "Return the schema for the arachne.chimera module"
  []
  schema/schema)

(defn configure
  "Configure the arachne.chimera module"
  [cfg]
  (-> cfg
    (migrations/add-root-migration)
    (migrations/ensure-migration-models)))

(deferror ::missing-op-spec
  :message "No spec found for operation type `:op-type`"
  :explanation "A Chimera operation could not be validated using Spec, because no spec could be found for the operatoin type `:op-type`"
  :suggestions ["Ensure that the type `:op-type` is correct and typo-free."
                "If you are a module author, confirm that your module registers a spec for `:op-type`"]
  :ex-data-docs {:op-type "The Chimera operation type"})

(deferror ::failed-op-spec
  :message "Operation data for `:op-type` did not conform to operation spec"
  :explanation "The system attempted to call a Chimera operation of type `:op-type`. However, the payload data that was passed to the operation failed to conform to the spec that had been registered for `:op-type`."
  :suggestions ["Ensure that the operation's payload data conforms to the declared specification."]
  :ex-data-docs {:op-type "The Chimera operation type"
                 :op-data "The data that failed to conform to the spec"})


(deferror ::unsupported-operation
  :message "Operation `:type` not supported by adapter of class `:adapter-class`"
  :explanation "The system attempted to call a Chimera operation of type `:type`. However, the specified adapter (Arachne ID: `:adapter-id`), of class `:adapter-class`, does not support that operation.

  The operations that it does support are:

  :supported-str

  "
  :suggestions ["Use a different, supported operation."
                "Use a different adapter or database that supports the operation you want."
                "Extend the adapter to provide support for the provided operation."]
  :ex-data-docs {:adapter "The adapter"
                 :adapter-id "The Arachne ID of the adapter"
                 :type "The operation type"
                 :adapter-class "The class of the adapter"
                 :supported "The operations that the adapter does support"
                 :supported-str "A bulleted list of supported operations (string)"})

(defn- assert-op-support
  "If the adapter is from a configuration, validate that the given operation is supported.

  If the adapter does not have a config attached, always return true (useful for REPL testing)"
  [adapter type]
  (when-let [cfg (:arachne/config adapter)]
    (let [supported (set (cfg/q cfg '[:find [?op ...]
                                      :in $ ?adapter
                                      :where
                                      [?adapter :chimera.adapter/capabilities ?cap]
                                      [?cap :chimera.adapter.capability/operation ?op]]
                           (:db/id adapter)))]
      (when-not (contains? supported type)
        (error ::unsupported-operation {:adapter adapter
                                        :adapter-id (:arachne/id adapter)
                                        :adapter-class (class adapter)
                                        :supported supported
                                        :supported-str (->> supported
                                                         (map #(str " - `" % "`"))
                                                         (interpose "\n")
                                                         (apply str))
                                        :type type})))))

(defn adapter? [obj] (satisfies? adapter/Adapter obj))

(s/fdef operate
  :args (s/cat :adapter adapter?
               :type :chimera/operation-type
               :data :chimera/operation-data))

(defn operate
  "Apply a Chimera operation to an adapter, first validating the operation."
  [adapter type data]
  (e/assert-args `operate adapter type data)
  (let [op-spec (s/get-spec type)]
    (when-not op-spec (error ::missing-op-spec {:op-type type}))
    (assert-op-support adapter type)
    (e/assert op-spec data ::failed-op-spec {:op-type type
                                             :op-data data})
    (adapter/operate- adapter type data)))


