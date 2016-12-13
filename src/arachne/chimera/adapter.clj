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

