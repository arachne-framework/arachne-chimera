(ns arachne.chimera
  (:require [clojure.spec :as s]
            [clojure.walk :as w]
            [arachne.error :as e :refer [error deferror]]
            [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.core.runtime :as rt]
            [arachne.chimera.specs]
            [arachne.chimera.schema :as schema]
            [arachne.chimera.operation :as ops]
            [arachne.chimera.migration :as migration]
            [arachne.chimera.adapter :as adapter]
            [valuehash.api :as vh]))

(defn ^:no-doc schema
  "Return the schema for the arachne.chimera module"
  []
  schema/schema)

(defn ^:no-doc configure
  "Configure the arachne.chimera module"
  [cfg]
  (-> cfg
    (ops/add-operations)
    (adapter/add-adapter-constructors)
    (migration/add-root-migration)
    (migration/ensure-migration-models)))

(deferror ::missing-op-spec
  :message "No function spec found for operation type `:op-type`"
  :explanation "A Chimera operation could not be validated using Spec, because no spec could be found for the operation type `:op-type`.

  Chimera expects that every operation type have a function spec registered, with both `:args` and `:ret`. This spec should define the behavior of the `arachne.chimera/operate` function for that operation type. "
  :suggestions ["Ensure that the type `:op-type` is correct and typo-free."
                "If you are a module author, confirm that your module registers a spec for `:op-type`"
                "Ensure that the spec is a valid function spec."]
  :ex-data-docs {:op-type "The Chimera operation type"})

(deferror ::failed-op-spec
  :message "Operation data for `:op-type` did not conform to operation spec"
  :explanation "The system attempted to call a Chimera operation of type `:op-type`. However, the data that was passed to the `operate` function failed to conform to the spec that had been registered for `:op-type`."
  :suggestions ["Ensure that the operation's payload data conforms to the declared specification."
                "Ensure that the other arguments to the `operate` function are correct."]
  :ex-data-docs {:op-type "The Chimera operation type"})

(defn ^:no-doc adapter? [obj] (satisfies? adapter/Adapter obj))

(deferror ::adapter-not-found
  :message "Could not find adapter `:lookup` in the specified runtime."
  :explanation "Some code made an attempt to look up an adapter identified by `:lookup` in an Arachne runtime. However, the runtime did not contain any such entity."
  :suggestions ["Ensure that the adapter lookup is correct, with no typos"
                "Ensure that the configuration actually contains the requested entity and that it is an adapter component."]
  :ex-data-docs {:rt "The runtime"
                 :lookup "Lookup expression for the adapter"})

(deferror ::failed-result-spec
  :message "Operation `:op-type` returned an invalid result."
  :explanation "The result of an `:op-type` operation did not match the operation's defined spec.

  The adapter was `:adapter-eid` (AID: `:adapter-aid`).

  This check was performed because the `arachne.chimera/*validate-operation-results*` dynamic var was set to true."
  :suggestions ["Fix the operation implementation in the given adapter"]
  :ex-data-docs {:op-type "Operation type"
                 :adapter-aid "Adapter Arachne ID"
                 :adapter-eid "Adapter Entity ID"})

(s/fdef operate
        :args (s/cat :adapter adapter?
                     :type :chimera/operation-type
                     :payload :chimera/operation-payload
                     :batch-context (s/? any?)))

(def ^:dynamic *validate-operation-results*
  "Bind to true to validate the data returned by operations against the
   operation spec. Useful mostly for testing of adapters."
  false)

(defn- conform-operation
  [& args]
  (let [op-type (second args)
        op-spec (s/get-spec op-type)]
    (when-not (:args op-spec) (error ::missing-op-spec {:op-type op-type}))

    (e/conform (:args op-spec) args ::failed-op-spec {:op-type op-type})))

(defn operate
  "Send a Chimera operation to an adapter, first validating the operation.

  The return value is the result of the operation. See the operation
  specification for what this entails.

  Optionally takes a fourth argument, the context of the operation. The
  context is an implementation-dependent value which is used when composing
  operations (such as batches or migrations.

  Some operations always require a context, some never do, and some may be
  called with or without one. See the operation specification for details."
  ([adapter type payload]
   (adapter/assert-operation-support adapter type false)
   (let [conformed (conform-operation adapter type payload)
         payload (if (instance? clojure.lang.IObj payload)
                   (vary-meta payload assoc ::conformed (:payload conformed))
                   payload)
         result (adapter/operate- adapter type payload)]
     (when *validate-operation-results*
       (e/assert (:ret (s/get-spec type)) result
         ::failed-result-spec {:op-type type
                               :adapter-eid (:db/id adapter)
                               :adapter-aid (:arachne/id adapter)}))
     result))

  ([adapter type payload context]
   (adapter/assert-operation-support adapter type true)
   (let [conformed (conform-operation adapter type payload context)
         result (adapter/operate- adapter type
                  (vary-meta payload assoc ::conformed (:payload conformed))
                  context)]
     (when *validate-operation-results*
       (e/assert (:ret (s/get-spec type)) result
         ::failed-result-spec {:op-type type
                               :adapter-eid (:db/id adapter)
                               :adapter-aid (:arachne/id adapter)}))
     result)))

(defn- migration-operations
  "Given a migration entity map, return a sequence of the migration operations"
  [migration]
  (map #(dissoc % :chimera.migration.operation/next)
    (take-while identity
      (iterate :chimera.migration.operation/next
        (:chimera.migration/operation migration)))))

(defn ensure-migrations
  "Given a config and a lookup for an adapter, ensure that all the adapter's
   migrations have been applied, applying any that have not."
  [rt adapter-lookup]
  (let [cfg (:config rt)
        adapter (rt/lookup rt adapter-lookup)]
    (when-not (adapter? adapter) (error ::adapter-not-found
                                   {:rt rt, :lookup adapter-lookup}))
    (operate adapter :chimera.operation/initialize-migrations true)
    (let [migration-eids (migration/migrations cfg (:db/id adapter))
          migrations (map #(migration/canonical-migration cfg %) migration-eids)]
      (doseq [migration migrations]
        (operate adapter :chimera.operation/migrate
          {:signature (vh/md5-str migration)
           :name (:chimera.migration/name migration)
           :operations (migration-operations migration)})))))


(defrecord Lookup [attribute value])

(defn lookup
  "Construct a Chimera lookup key"
  ([identifier]
   (if (instance? Lookup identifier)
     identifier
     (apply ->Lookup identifier)))
  ([attr value] (->Lookup attr value)))

(defn lookup?
  "Determine whether an object is a Chimera Lookup"
  [obj]
  (instance? Lookup obj))

(defn entity-lookup
  "Given an adapter and an entity map, return a Lookup key.

   Throws an exception if the entity map does not contain an identity attribute."
  [adapter entity-map]
  (if-let [key (first (filter #(adapter/key? adapter %) (keys entity-map)))]
    (lookup key (get entity-map key))
    (let [model-keys (adapter/key-attributes adapter)]
      (error ::ops/no-key-specified
        {:adapter-eid (:db/id adapter)
         :adapter-aid (:arachne/id adapter)
         :provided-attrs model-keys
         :provided-attrs-str (e/bullet-list (keys entity-map))
         :key-attrs model-keys
         :key-attrs-str (e/bullet-list model-keys)}))))