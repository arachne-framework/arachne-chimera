(ns arachne.chimera
  (:require [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.chimera.schema :as schema]
            [arachne.chimera.migration :as migrations]
            [arachne.chimera.adapter :as adapters]
            ))

(set! *print-namespace-maps* false)

(defn schema
  "Return the schema for the arachne.chimera module"
  []
  schema/schema)

(defn configure
  "Configure the arachne.chimera module"
  [cfg]
  (-> cfg
    (migrations/add-root-migration)
    (adapters/ensure-model-for-all)))
