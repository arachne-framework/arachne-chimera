(ns arachne.chimera.adapter.config
  "An adapter that provides read-only access to the Arachne config itself"
  (:require [clojure.spec :as s]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core :as core]
            [arachne.core.util :as util]
            [arachne.core.config.specs :as cfg-specs]
            [arachne.core.config :as cfg]
            [arachne.core.config.init :as script :refer [defdsl]]
            [arachne.chimera.specs :as cs]
            [arachne.chimera.adapter :as adapter]
            [arachne.chimera.migration :as mig]))

(defn- all-types
  "Find all the types present in the Arachne config"
  [cfg]
  (cfg/q cfg '[:find [?t ...]
               :in $ %
               :where
               (type ?t)
               [?t :db/ident ?ident]]
    '[[(type ?t)
      [_ :arachne.attribute/domain ?t]]
      [(type ?t)
       [_ :arachne.attribute/range ?t]]
      [(type ?t)
       [_ :arachne.type/supertypes ?t]]
      [(type ?t)
      [?t :arachne.type/supertypes _]]]))

(defn- extract-model-types
  [raw-types]
  (into {}
    (map (fn [t]
           [(:db/ident t) (util/mkeep
                            {:chimera.type/name (:db/ident t)
                             :chimera.type/supertypes
                             (map :db/ident (:arachne.type/supertypes t))})])
      raw-types)))

(def ^:private prim-mapping
  {:db.type/boolean :chimera.primitive/boolean
   :db.type/string  :chimera.primitive/string
   :db.type/keyword :chimera.primitive/keyword
   :db.type/long    :chimera.primitive/long
   :db.type/double  :chimera.primitive/double
   :db.type/bigdec  :chimera.primitive/bigdec
   :db.type/bigint  :chimera.primitive/bigint
   :db.type/instant :chimera.primitive/instant
   :db.type/uuid    :chimera.primitive/uuid
   :db.type/bytes   :chimera.primitive/bytes})

(defn- extract-model-attr
  "Extract a model attributes from an Arachne config attr to a Chimera config
  attr"
  [attr]
  (let [name (:db/ident attr)
        domain (:db/ident (:arachne.attribute/domain attr))
        range (or (:db/ident (:arachne.attribute/range attr))
                  (prim-mapping (:db/ident (:db/valueType attr))))
        min-card (or (:arachne.attribute/min-cardinality attr) 0)
        max-card (or (:arachne.attribute/max-cardinality attr)
                     (when (= :db.cardinality/one
                              (:db/ident (:db/cardinality attr)))))
        key (when (= :db.unique/identity (:db/ident (:db/unique attr)))
              true)
        component (when (:db/isComponent attr) true)]
    (util/mkeep
      {:chimera.attribute/name name
       :chimera.attribute/domain domain
       :chimera.attribute/min-cardinality min-card
       :chimera.attribute/max-cardinality max-card
       :chimera.attribute/range range
       :chimera.attribute/key key
       :chimera.attribute/component component})))

(defn- extract-model-attrs
  "Extract the model attributes from an Arachne config model to a Chimera model
  format"
  [raw-types]
  (->> raw-types
    (mapcat :arachne.attribute/_domain)
    (map extract-model-attr)
    (map (fn [attr] [(:chimera.attribute/name attr) attr]))
    (into {})))

(def ^:private type-pull-expr
  '[:db/ident
    {:arachne.type/supertypes ...
     :arachne.attribute/_domain [* {:arachne.attribute/range [:db/ident]
                                    :db/cardinality [:db/ident]
                                    :arachne.attribute/domain [:db/ident]
                                    :db/valueType [:db/ident]
                                    :db/unique [:db/ident]}]}])

(defn- model
  "Build a data model based on the Arachne configuration's model"
  [cfg]
  (let [types (map #(cfg/pull cfg type-pull-expr %)
                (all-types cfg))]
    {:types (extract-model-types types)
     :attrs (extract-model-attrs types)}))

(defn populate-config-adapters
  "Populate the data model for config adapters present in the config."
  [cfg]
  (let [adapters (cfg/q cfg '[:find [?a ...]
                              :where [?a :chimera.adapter/type
                                      :arachne.chimera.adapter/config]])
        data-model (model cfg)]
    (if (empty? adapters)
      cfg
      (cfg/with-provenance :module `populate-config-adapters
        (cfg/update cfg
          (mapcat (fn [adapter-eid]
                    (conj (adapter/add-data-model data-model adapter-eid)
                      {:db/id adapter-eid
                       :chimera.adapter/capabilities
                       [{:chimera.adapter.capability/operation :chimera.operation/get
                         :chimera.adapter.capability/atomic true
                         :chimera.adapter.capability/idempotent true
                         :chimera.adapter.capability/transactional true}],
                       :arachne.component/constructor
                       :arachne.chimera.adapter.config/ctor}))
            adapters))))))

(defrecord ConfigAdapter [cfg]
  adapter/Adapter
  (operate- [this type [attr value]]
    (let [eids (cfg/q cfg '[:find [?e ...]
                            :in $ ?a ?v
                            :where
                            [?e ?a ?v]]
                 attr value)]
      (map (fn [eid]
             (cfg/pull cfg '[*] eid))
        eids))))

(defn ctor
  "Constructor for a config adapter component"
  [cfg _]
  (->ConfigAdapter cfg))

