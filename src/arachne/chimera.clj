(ns arachne.chimera
  (:require [arachne.core.util :as util]
            [arachne.core.config :as cfg]
            [arachne.chimera.schema :as schema]
            ))

(defn schema
  "Return the schema for the arachne.chimera module"
  []
  schema/schema)

(defn configure
  "Configure the arachne.chimera module"
  [cfg]
  (cfg/with-provenance :module `configure
    (cfg/update cfg
      [{:db/id (cfg/tempid)
        :chimera.migration/name :chimera/root-migration
        :chimera.migration/doc "Chimera's default root migration"}])))
