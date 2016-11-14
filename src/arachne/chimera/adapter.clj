(ns arachne.chimera.adapter
  (:require [clojure.spec :as s]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core :as core]
            [arachne.core.config.specs :as cfg-specs]
            [arachne.core.config :as cfg]
            [arachne.core.config.init :as script :refer [defdsl]]
            [arachne.chimera.specs :as cs]
            [arachne.chimera.migration :as mig]))

(defn ensure-model-for-all
  "For every adapter in the configuration, ensure that it has a model associated
  with it, based on its migrations."
  [cfg]
  (let [adapters (cfg/q cfg '[:find [?a ...]
                              :in $
                              :where
                              [?a :chimera.adapter/migrations _]
                              [(missing? $ ?a :chimera.adapter/model)]])]
    (cfg/with-provenance :module `ensure-model-for-all
      (reduce (fn [cfg adapter-eid]
                (mig/build-data-model cfg adapter-eid))
        cfg adapters))))