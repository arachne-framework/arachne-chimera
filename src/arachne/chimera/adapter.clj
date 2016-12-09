(ns arachne.chimera.adapter
  (:require [clojure.spec :as s]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core :as core]
            [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.chimera.specs :as cs]
            [arachne.chimera.migration :as mig]))

(defprotocol Adapter
  "An Arachne component that represents an interface to a data store."
  (operate- [this type data]
    "Perform an operation against the data store specified by this adapter."))

(defn- add-db-id
  "Adds a tempid to the entity map if not already present"
  [em]
  (if (:db/id em) em (assoc em :db/id (cfg/tempid))))

(defn add-data-model
  "Given a data model and the entity ID of an adapter, return txdata to
  add the data model to the specified adapter."
  [data-model adapter-eid]
  (let [model-elements (concat (vals (:types data-model))
                                 (vals (:attrs data-model)))
          model-elements (map add-db-id model-elements)
        model-elements (map util/mkeep model-elements)
        eids (map :db/id model-elements)
        txdata (conj model-elements {:db/id adapter-eid
                                     :chimera.adapter/model eids})]
    txdata))

(defn ensure-migration-models
  "For every adapter in the configuration, ensure that it has a model associated
  with it, based on its migrations."
  [cfg]
  (let [adapters (cfg/q cfg '[:find ?a ?m
                              :in $
                              :where
                              [?a :chimera.adapter/migrations ?m]
                              [(missing? $ ?a :chimera.adapter/model)]])]
    (cfg/with-provenance :module `ensure-model-for-all
      (reduce (fn [cfg [adapter-eid adapter-migrations]]
                (let [migs (map second adapter-migrations)
                      model (mig/rollup cfg migs)]
                  (cfg/update cfg (add-data-model model adapter-eid))))
        cfg (group-by first adapters)))))

