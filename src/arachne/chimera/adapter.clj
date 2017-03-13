(ns arachne.chimera.adapter
  (:require [clojure.spec :as s]
            [clojure.core.match :as m]
            [arachne.error :as e :refer [deferror error]]
            [arachne.error.format :as efmt]
            [clojure.string :as str]
            [arachne.core :as core]
            [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.chimera.specs :as cs]
            [arachne.chimera.migration :as mig])
  (:import [java.util WeakHashMap]))

(defprotocol Adapter
  "An object representing an interface to some storage mechanism/database"
  (operate- [this operation-type payload]
            [this operation-type payload batch-context]
    "Perform a Chimera operation on this adapter."))

(defrecord ChimeraAdapter [dispatch]
  Adapter
  (operate- [this operation-type payload]
    (dispatch this operation-type payload))
  (operate- [this operation-type payload batch-context]
    (dispatch this operation-type payload batch-context)))

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

(defn missing-dispatch
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

(defn ctor
  "Constructor function for all adapter components"
  [cfg eid]
  (->ChimeraAdapter (build-dispatch-fn cfg eid)))

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

(defn weak-memoize
  "Memoize a function using a weak hash map"
  [f]
  (let [cache (WeakHashMap.)]
    (fn [& args]
      (if-let [v (get cache args)]
        v
        (let [result (apply f args)]
          (.put cache args result)
          result)))))

(defn supported-operations
  "Return the operation types that the given adapter supports, as a map of {<type> <batchable?>}"
  [adapter]
  (into {} (cfg/q (:arachne/config adapter)
                  '[:find ?type ?batchable
                    :in $ ?adapter
                    :where
                    [?adapter :chimera.adapter/capabilities ?cap]
                    [?cap :chimera.adapter.capability/operation ?op]
                    [?op :chimera.operation/type ?type]
                    [?op :chimera.operation/batchable? ?batchable]]
                  (:db/id adapter))))

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

(def supported-operations-mem (weak-memoize supported-operations))

(defn assert-operation-support
  "Validate that the given operation is supported by the specified adapter."
  [adapter operation-type batch?]
  (let [supported (supported-operations-mem adapter)]
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


(defn model-keys
  "Return a set of primary keys in the adapter's model"
  [adapter]
  (set
   (cfg/q (:arachne/config adapter)
          '[:find [?key ...]
            :in $ ?adapter
            :where
            [?adapter :chimera.adapter/model ?attr]
            [?attr :chimera.attribute/key true]
            [?attr :chimera.attribute/name ?key]]
          (:db/id adapter))))

(defn component-attrs
  "Return a set of attributes that are component refs, in the adapter's model"
  [adapter]
  (set
   (cfg/q (:arachne/config adapter)
          '[:find [?attr-name ...]
            :in $ ?adapter
            :where
            [?adapter :chimera.adapter/model ?attr]
            [?attr :chimera.attribute/component true]
            [?attr :chimera.attribute/name ?attr-name]]
          (:db/id adapter))))