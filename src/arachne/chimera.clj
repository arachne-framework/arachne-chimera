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
            [valuehash.api :as vh])
  (:refer-clojure :exclude [key get update]))

(defn ^:no-doc schema
  "Return the schema for the arachne.chimera module"
  []
  schema/schema)

(defn ^:no-doc configure
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

(defn ^:no-doc adapter? [obj] (satisfies? adapter/Adapter obj))

(deferror ::adapter-not-found
  :message "Could not find adapter `:lookup` in the specified runtime."
  :explanation "Some code made an attempt to look up an adapter identified by `:lookup` in an Arachne runtime. However, the runtime did not contain any such entity."
  :suggestions ["Ensure that the adapter lookup is correct, with no typos"
                "Ensure that the configuration actually contains the requested entity and that it is an adapter component."]
  :ex-data-docs {:rt "The runtime"
                 :lookup "Lookup expression for the adapter"})

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
        (operate adapter :chimera.operation/migrate [(vh/md5-str migration) migration])))))


(defrecord Lookup [attribute value])

(defn lookup
  "Construct a Chimera lookup key"
  ([identifier]
   (if (instance? Lookup identifier)
     identifier
     (apply ->Lookup identifier)))
  ([attr value] (->Lookup attr value)))

(s/fdef put
  :args (s/cat :adapter adapter?
               :entity-map :chimera/entity-map))

(defn put
  "Insert a new Chimera entity into a data source. The entity map must have at least one 'key'
   attribute. The key attribute should not be present in the database already; `put` is not
   intended for updates and will fail if the entity already exists. "
  [adapter entity-map]
  (operate adapter :chimera.operation/put entity-map))

(s/fdef get
 :args (s/cat :adapter adapter?
              :lookup (s/alt :lookup-record :chimera/lookup
                             :tuple (s/tuple :chimera.attribute/name :chimera/primitive)
                             :attr-val (s/cat :attribute :chimera.attribute/name
                                              :value :chimera/primitive))))

(defn get
  "Retrieve the specified Chimera entity map from a data source.

   The entity map will contain all of the entity's attributes. Values of ref attributes will be Lookups."
  ([adapter identifier]
   (operate adapter :chimera.operation/get (lookup identifier)))
  ([adapter key value]
   (operate adapter :chimera.operation/get (lookup key value))))

(s/fdef update
        :args (s/cat :adapter adapter?
                     :entity-map :chimera/entity-map))

(defn update
  "Update a Chimera entity in a data source. The entity map must have at least one 'key'
   attribute. An entity with the provided key must already be present in the database, `update` can
   only modify existing records, not create new ones.

   The entity will be updated with any other attributes present on the entity map. Cardinality-one
   attributes will be changed to reflect the new value. Cardinality-many attributes will have a
   value added (no old values will be removed)."
  [adapter entity-map]
  (operate adapter :chimera.operation/update entity-map))

(s/fdef delete
        :args (s/cat :adapter adapter?
                     :lookup (s/alt :lookup-record :chimera/lookup
                                    :tuple (s/tuple :chimera.attribute/name :chimera/primitive)
                                    :attr-val (s/cat :attribute :chimera.attribute/name
                                                     :value :chimera/primitive))))

(defn delete
  "Remove the specified Chimera entity and all its attributes from a data source.

   Any component attributes will also be deleted.

   The provided entity must exist; entities which do not exist cannot be deleted."
  ([adapter identifier]
   (operate adapter :chimera.operation/delete (lookup identifier)))
  ([adapter key value]
   (delete adapter [key value])))

