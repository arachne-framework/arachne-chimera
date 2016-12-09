(ns arachne.chimera.adapter.config.dsl
  "DSL elements for creating config adapters in the config"
  (:require [clojure.spec :as s]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core :as core]
            [arachne.core.dsl.specs :as core-specs]
            [arachne.core.util :as util]
            [arachne.core.config.specs :as cfg-specs]
            [arachne.core.config :as cfg]
            [arachne.core.config.init :as script :refer [defdsl]]
            [arachne.chimera.specs :as cs]
            [arachne.chimera.adapter :as adapter]
            [arachne.chimera.migration :as mig]))

(s/fdef config-adapter
  :args (s/cat :arachne-id ::core-specs/id))

(defdsl config-adapter
  "Create a config adapter entity in the configuration."
  [arachne-id]
  (let [tempid (cfg/tempid)]
    (cfg/resolve-tempid
      (script/transact [{:db/id tempid
                         :arachne/id arachne-id
                         :chimera.adapter/type :arachne.chimera.adapter/config}])
      tempid)))