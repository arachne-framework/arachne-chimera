(ns arachne.chimera
  (:require [clojure.spec :as s]
            [clojure.walk :as w]
            [arachne.error :as e :refer [error deferror]]
            [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.core.runtime :as rt]
            [arachne.chimera.specs]
            [arachne.chimera.schema :as schema]
            [arachne.chimera.migration :as migration]
            [arachne.chimera.adapter :as adapter]
            [valuehash.api :as vh]))

(defn schema
  "Return the schema for the arachne.chimera module"
  []
  schema/schema)

(defn configure
  "Configure the arachne.chimera module"
  [cfg]
  (-> cfg
    (adapter/add-adapter-constructors)
    (migration/add-root-migration)
    (migration/ensure-migration-models)))

(deferror ::missing-op-spec
  :message "No spec found for operation type `:op-type`"
  :explanation "A Chimera operation could not be validated using Spec, because no spec could be found for the operation type `:op-type`"
  :suggestions ["Ensure that the type `:op-type` is correct and typo-free."
                "If you are a module author, confirm that your module registers a spec for `:op-type`"]
  :ex-data-docs {:op-type "The Chimera operation type"})

(deferror ::failed-op-spec
  :message "Operation data for `:op-type` did not conform to operation spec"
  :explanation "The system attempted to call a Chimera operation of type `:op-type`. However, the payload data that was passed to the operation failed to conform to the spec that had been registered for `:op-type`."
  :suggestions ["Ensure that the operation's payload data conforms to the declared specification."]
  :ex-data-docs {:op-type "The Chimera operation type"
                 :op-payload "The payload data that failed to conform to the spec"})

(defn adapter? [obj] (satisfies? adapter/Adapter obj))

(s/fdef operate
  :args (s/cat :adapter adapter?
               :type :chimera/operation-type
               :payload :chimera/operation-payload))

(defn operate
  "Apply a Chimera operation to an adapter, first validating the operation."
  [adapter type payload]
  (e/assert-args `operate adapter type payload)
  (adapter/assert-operation-support adapter type)
  (let [op-spec (s/get-spec type)]
    (when-not op-spec (error ::missing-op-spec {:op-type type}))
    (e/assert op-spec payload ::failed-op-spec {:op-type type
                                                :op-payload payload})
    (adapter/operate- adapter type payload)))


(deferror ::adapter-not-found
  :message "Could not find adapter `:lookup` in the specified runtime."
  :explanation "Some code made an attempt to look up an adapter identified by
  `:lookup` in an Arachne runtime. However, the runtime did not contain any such entity."
  :suggestions ["Ensure that the adapter lookup is correct, with no typos"
                "Ensure that the configuration actually contains the requested entity and that it is an adapter component."]
  :ex-data-docs {:rt "The runtime"
                 :lookup "Lookup expression for the adapter"})

(defn ensure-migrations
  "Given a config and a lookup for an adapter, ensure that all the adapter's
   migrations have been applied."
  [rt adapter-lookup]
  (let [cfg (:config rt)
        adapter (rt/lookup rt adapter-lookup)]
    (when-not (adapter? adapter) (error ::adapter-not-found
                                   {:rt rt, :lookup adapter-lookup}))
    (operate adapter :chimera.operation/initialize-migrations true)
    (let [migration-eids (migration/migrations cfg (:db/id adapter))
          migrations (map #(migration/canonical-migration cfg %) migration-eids)]
      (doseq [migration migrations]
        (operate adapter :chimera.operation/migrate [(vh/md5-str migration) migration])))))