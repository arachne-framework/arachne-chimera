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
    "Perform a Chimera operation on this adapter."))

(defrecord ChimeraAdapter [dispatch]
  Adapter
  (operate- [this operation-type payload]
    (dispatch this operation-type payload)))

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
  [adapter op payload]
  (let [dispatches (cfg/q (:arachne/config adapter)
                     '[:find ?i ?p ?impl
                       :in $ ?adapter
                       :where
                       [?a :chimera.adapter/dispatches ?d]
                       [?d :chimera.adapter.dispatch/index ?i]
                       [?d :chimera.adapter.dispatch/pattern ?p]
                       [?d :chimera.adapter.dispatch/impl ?impl]]
                     (:db/id adapter))
        dispatches (sort-by first > dispatches)
        dispatch-strings (map (fn [[_ p i]]
                                    (str "`" p " " i "`"))
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
  (let [dispatches (cfg/q cfg '[:find ?i ?pattern ?impl
                                :in $ ?adapter
                                :where
                                [?adapter :chimera.adapter/dispatches ?d]
                                [?d :chimera.adapter.dispatch/index ?i]
                                [?d :chimera.adapter.dispatch/pattern ?pattern]
                                [?d :chimera.adapter.dispatch/impl ?impl]]
                     adapter-eid)
        dispatches (sort-by first > dispatches)
        dispatch-clauses (mapcat (fn [[_ pattern fn]]
                                   [(read-string pattern)
                                    (list (symbol (namespace fn) (name fn))
                                      'adapter, 'op, 'payload)])
                           dispatches)
        dispatch-fn `(fn [~'adapter ~'op ~'payload]
                       (m/match [~'op ~'payload]
                         ~@dispatch-clauses
                         :else (missing-dispatch ~'adapter ~'op ~'payload)))]
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

(let [cache (WeakHashMap.)]
  (defn- supported-operations
    "Return the operations that the given adapter supports.

    Memoized using a WeakHashMap."
    [adapter]
    (if-let [v (get cache adapter)]
      v
      (do
        (let [v (set (cfg/q (:arachne/config adapter)
                       '[:find [?op ...]
                         :in $ ?adapter
                         :where
                         [?adapter :chimera.adapter/capabilities ?cap]
                         [?cap :chimera.adapter.capability/operation ?op]]
                       (:db/id adapter)))]
          (.put cache adapter v)
          v)))))

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

(defn assert-operation-support
  "Validate that the given operation is supported by the specified adapter."
  [adapter operation-type]
  (let [supported (supported-operations adapter)
        supported-str  (->> supported
                         (map #(str "  - `" % "`"))
                         (str/join "\n"))]
    (when-not (contains? supported operation-type)
      (error ::unsupported-operation {:adapter adapter
                                      :adapter-eid (:db/id adapter)
                                      :adapter-aid (:arachne/id adapter)
                                      :supported supported
                                      :supported-str supported-str
                                      :type operation-type}))))

;; THIS IS WHAT WE'RE BUILDING
(comment



  ;; In-code API:
  (operate adapter :chimera.operation/get [:myapp/Person :person/id 42])

  ;; In module configuration (as data, will rarely be entered this way)
  {:chimera.adapter/capabilities {}
   :chimera.adapter/migrations []
   :chimera.adapter/impls [{:chimera.adapter.impl/index 0
                            :chimera.adapter.impl/pattern "[:chimera.operation/get _]"
                            :chiemra.adapter.impl/fn :chimera.adapter.datomic-peer/get}
                           {:chimera.adapter.impl/index 1
                            :chimera.adapter.impl/pattern "[:chimera.operation/put _]"
                            :chiemra.adapter.impl/fn :chimera.adapter.datomic-peer/put}
                           {:chimera.adapter.impl/index 2
                            :chimera.adapter.impl/pattern "[:chimera.operation/batch _]"
                            :chiemra.adapter.impl/fn :chimera.adapter.datomic-peer/batch}]}

  ;; User-facing DSL
  (datomic-adapter :myapp/datomic
    :migrations [:myapp.migrations/foo]
    :uri "datomic:mem://foobar")

  (extend-adapter :myapp/datomic
    [:chimera.operation/get [:myapp/Person :person/id _]] 'myapp/get-person
    [:chimera.operation/put [:myapp/Person _]] 'myapp/put-person)
  )
