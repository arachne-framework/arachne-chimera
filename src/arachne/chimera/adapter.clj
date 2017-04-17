(ns arachne.chimera.adapter
  (:require [clojure.spec :as s]
            [clojure.core.match :as m]
            [clojure.pprint :as pprint]
            [arachne.error :as e :refer [deferror error]]
            [arachne.error.format :as efmt]
            [clojure.string :as str]
            [arachne.core :as core]
            [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.chimera.specs :as cs]
            [arachne.chimera.migration :as mig]
            [arachne.chimera.operation :as o]
            [com.stuartsierra.component :as c]
            [arachne.core.util :as u])
  (:import [java.util WeakHashMap]))

(defprotocol Adapter
  "An object representing an interface to some storage mechanism/database"
  (operate- [this operation-type payload]
            [this operation-type payload batch-context]
    "Perform a Chimera operation on this adapter."))

(defrecord ChimeraAdapter [dispatch supported-operations attributes]
  c/Lifecycle
  (start [this]
    (if-let [start (:chimera.adapter/start this)]
      (let [start-fn (u/require-and-resolve start)]
        (start-fn this))
      this))
  (stop [this]
    (if-let [stop (:chimera.adapter/stop this)]
      (let [stop-fn (u/require-and-resolve stop)]
        (stop-fn this))
      this))
  Adapter
  (operate- [this operation-type payload]
    (dispatch this operation-type payload))
  (operate- [this operation-type payload batch-context]
    (dispatch this operation-type payload batch-context))
  Object
  (toString [this]
    (str "#ChimeraAdapter[" (:db/id this) (when (:arachne/id this)
                                            (str " " (:arachne/id this))) "]")))

(defmethod print-method ChimeraAdapter
  [v ^java.io.Writer w]
  (.write w (.toString v)))

(defmethod pprint/simple-dispatch ChimeraAdapter
  [cfg]
  (pr cfg))

(prefer-method pprint/simple-dispatch
  arachne.chimera.adapter.ChimeraAdapter
  clojure.lang.IPersistentMap)

(deferror ::missing-dispatch
  :message "Unknown dispatch operation `:op` for adapter `:adapter-eid` (Arachne ID: `:adapter-aid`)"
  :explanation "Operations to a Chimera Adapter are dispatched using core.match expressions, derived from data stored in the Arachne config.

  The requested operation was `:op`.

  The requested payload was:

  `:payload-str`

  The patterns actually supported by the adapter are:

  :supported-str"
  :suggestions ["Ensure the operation type and payload data are correct"
                "If you are the author of the adapter in question, ensure that it supports the desired operation."
                "If you know what you're doing, override the operation by adding an override dispatch to the adapter in your Arachne config."]
  :ex-data-docs {:adapter "the adapter"
                 :adapter-eid "eid of the adapter"
                 :adapter-aid "Arachne ID of the adapter"
                 :supported-str "pprinted supported dispatches"
                 :op "operation type"
                 :payload "operation payload data"
                 :payload-str "pprinted payload data"})

(defn ^:no-doc missing-dispatch
  "Throw an error for a missing dispatch"
  [adapter op payload all-dispatches]
  (let [dispatches (sort-by first > all-dispatches)
        dispatch-strings (map (fn [[_ _ op pattern impl]]
                                    (str "`[" op " " pattern "] " impl "`"))
                           dispatches)
        dispatch-string (str/join "\n" dispatch-strings)]
    (error ::missing-dispatch {:adapter adapter
                               :adapter-eid (:db/id adapter)
                               :adapter-aid (:arachne/id adapter)
                               :supported-str dispatch-string
                               :op op
                               :payload payload
                               :payload-str (efmt/pprint-str-truncated payload 10)})))

(defn- build-dispatch-fn
  "Based on the dispatches defined in the config, build and return a function
  that will handle adapter operations"
  [cfg adapter-eid]
  (let [dispatches (cfg/q cfg '[:find ?idx ?batchable ?op ?pattern ?impl
                                :in $ ?adapter
                                :where
                                [?a :chimera.adapter/dispatches ?d]
                                [?d :chimera.adapter.dispatch/index ?idx]
                                [?d :chimera.adapter.dispatch/pattern ?pattern]
                                [?d :chimera.adapter.dispatch/impl ?impl]
                                [?d :chimera.adapter.dispatch/operation ?o]
                                [?o :chimera.operation/type ?op]
                                [?o :chimera.operation/batchable? ?batchable]]
                          adapter-eid)
        dispatches (sort-by first > dispatches)
        batch-dispatches (filter second dispatches)
        dispatch-clauses (mapcat (fn [[_ _ op pattern impl]]
                                   [[op (read-string pattern)]
                                    (list (symbol (namespace impl) (name impl))
                                      'adapter, 'op, 'payload)])
                           dispatches)
        batch-dispatch-clauses (mapcat (fn [[_ _ op pattern impl]]
                                         [[op (read-string pattern)]
                                          (list (symbol (namespace impl) (name impl))
                                                'adapter, 'op, 'payload 'batch-context)])
                                       dispatches)
        dispatch-fn `(fn
                       ([~'adapter ~'op ~'payload]
                        (m/match [~'op ~'payload]
                                 ~@dispatch-clauses
                                 :else (missing-dispatch ~'adapter ~'op ~'payload ~(vec dispatches))))
                       ([~'adapter ~'op ~'payload ~'batch-context]
                        (m/match [~'op ~'payload]
                                 ~@batch-dispatch-clauses
                                 :else (missing-dispatch ~'adapter ~'op ~'payload ~(vec dispatches)))))]
    (eval dispatch-fn)))

(defn- supported-operations
  "Return the operation types that the given adapter supports, as a map of {<type> <batchable?>}"
  [cfg adapter-eid]
  (into {} (cfg/q cfg
                  '[:find ?type ?batchable
                    :in $ ?adapter
                    :where
                    [?adapter :chimera.adapter/capabilities ?cap]
                    [?cap :chimera.adapter.capability/operation ?op]
                    [?op :chimera.operation/type ?type]
                    [?op :chimera.operation/batchable? ?batchable]]
                  adapter-eid)))

(defn- attributes
  "Return a map of attribute names to attribute entity maps, for all attributes known to the Adapter"
  [cfg adapter-eid]
  (let [attrs (cfg/q cfg '[:find [?attr ...]
                           :in $ ?adapter
                           :where
                           [?adapter :chimera.adapter/model ?attr]
                           [?attr :chimera.attribute/name _]]
                     adapter-eid)
        attrs (map #(cfg/pull cfg '[*] %) attrs)]
    (zipmap (map :chimera.attribute/name attrs)
            attrs)))

(defn ctor
  "Constructor function for all adapter components"
  [cfg eid]
  (->ChimeraAdapter (build-dispatch-fn cfg eid)
                    (supported-operations cfg eid)
                    (attributes cfg eid)))

(defn add-adapter-constructors
  "Ensure that every Adapter entity in the config has the correct constructor"
  [cfg]
  (let [adapters (cfg/q cfg '[:find [?a ...]
                              :where
                              [?a :chimera.adapter/capabilities _]])
        txdata (map (fn [adapter]
                      {:db/id adapter
                       :arachne.component/constructor ::ctor})
                 adapters)]
    (if (empty? txdata)
      cfg
      (cfg/with-provenance :module `add-adapter-constructors
        (cfg/update cfg txdata)))))

(deferror ::unsupported-operation
  :message "Operation `:type` not supported by adapter `:adapter-eid` (Arachne ID: `:adapter-aid`)"
  :explanation "The system attempted to call a Chimera operation of type `:type`. However, the specified adapter `:adapter-eid` (Arachne ID: `:adapter-aid`) does not support that operation, as defined in its configuration data.

  The operations that it does support are:

  :supported-str"
  :suggestions ["Use a different, supported operation."
                "Use a different adapter or database that supports the operation you want."
                "Extend the adapter to provide support for the provided operation."]
  :ex-data-docs {:adapter "The adapter"
                 :adapter-aid "The Arachne ID of the adapter"
                 :adapter-eid "The entity ID of the adapter"
                 :type "The operation type"
                 :adapter-class "The class of the adapter"
                 :supported "The operations that the adapter does support"
                 :supported-str "A bulleted list of supported operations (string)"})

(deferror ::non-batch-operation
          :message "Operation `:type` is not a batch-able operation."
          :explanation "The system attempted to call a Chimera operation of type `:type`. However, the definition of that operation's semantics specifies that it is not intended to be run within a batch."
          :suggestions ["Use a different type of operation which is supported inside a batch."
                        "Use the operation separately, instead of inside a batch."]
          :ex-data-docs {:type "The operation type"})

(defn assert-operation-support
  "Validate that the given operation is supported by the specified adapter."
  [adapter operation-type batch?]
  (let [supported (:supported-operations adapter)]
    (when-not (contains? supported operation-type)
      (let [supported-str (->> supported
                            (map #(str "  - `" % "`"))
                            (str/join "\n"))]
        (error ::unsupported-operation {:adapter adapter
                                        :adapter-eid (:db/id adapter)
                                        :adapter-aid (:arachne/id adapter)
                                        :supported supported
                                        :supported-str supported-str
                                        :type operation-type})))
    (when batch?
      (when-not (supported operation-type)
        (error ::non-batch-operation
               {:type operation-type})))))

(defn key?
  "Given an adapter and an attribute name, return true if the attribute is a key in the adapter's model"
  [adapter attr]
  (-> adapter :attributes attr :chimera.attribute/key))

(defn key-attributes
  "Given an adapter, return a set of attribute names for all the keys in the adapter's model"
  [adapter]
  (->> adapter
       :attributes
       (map val)
       (filter :chimera.attribute/key)
       (map :chimera.attribute/name)
       (set)))

(defn component?
  "Given an adapter and an attibute name, return true if the attribute is a component attribute."
  [adapter attr]
  (-> adapter :attributes attr :chimera.attribute/component))

(def ^:private primitives
  "Primitive value types"
  #{:chimera.primitive/boolean
    :chimera.primitive/string
    :chimera.primitive/keyword
    :chimera.primitive/long
    :chimera.primitive/double
    :chimera.primitive/bigdec
    :chimera.primitive/bigint
    :chimera.primitive/instant
    :chimera.primitive/uuid
    :chimera.primitive/bytes})

(defn attr-range
  "Given an adapter and an attribute name, return the attribute's range.

  The range will either be a primitive keyword (for primitive attributes), or
  the type name (for refs)"
  [adapter attr]
  (-> adapter :attributes attr :chimera.attribute/range))

(defn ref?
  "Given an adapter and an attibute name, return true if the attribute is a ref attribute."
  [adapter attr]
  (when-let [r (attr-range adapter attr)]
    (not (primitives r))))

(defn cardinality-many?
  "Given an adapter and an attibute name, return true if the attribute permits multiple values."
  [adapter attr]
  (not (= 1 (-> adapter :attributes attr :chimera.attribute/max-cardinality))))

(defn key-for-type
  "Given an adapter and a type name, return the key attribute for that type."
  [adapter type]
  (cfg/q (:arachne/config adapter)
    '[:find ?attr .
      :in $ ?adapter ?type
      :where
      [?adapter :chimera.adapter/model ?dme]
      [?dme :chimera.attribute/domain ?type]
      [?dme :chimera.attribute/key true]
      [?dme :chimera.attribute/name ?attr]]
    (:db/id adapter) type))

(defn identity-attribute
  "Given an adapter, an operation type, and an entity map, return the identity
   attribute in the entity map, based on attributes defined as identity
   attributes for the adapter.

   Throws an exception if the entity map does not contain an identity attribute."
  [adapter entity-map op]
  (if-let [key (first (filter #(key? adapter %) (keys entity-map)))]
    key
    (let [model-keys (key-attributes adapter)]
      (error ::o/no-key-specified
        {:op op
         :adapter-eid (:db/id adapter)
         :adapter-aid (:arachne/id adapter)
         :provided-attrs model-keys
         :provided-attrs-str (e/bullet-list (keys entity-map))
         :key-attrs (key-attributes adapter)
         :key-attrs-str (e/bullet-list model-keys)}))))