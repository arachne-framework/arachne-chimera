(ns arachne.chimera.dsl
  (:refer-clojure :exclude [extend-type])
  (:require [clojure.spec :as s]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.config :as cfg]
            [arachne.core.config.init :as script :refer [defdsl]]))

(s/def ::migration-name (s/and keyword? namespace))
(s/def ::type-name (s/and keyword? namespace))
(s/def ::attribute-name (s/and keyword? namespace))

(s/def ::operation-txmap map?)

(s/fdef migration
  :args (s/cat :name ::migration-name
               :docstr string?
               :parents (s/coll-of ::migration-name)
               :ops (s/+ ::operation-txmap)))


(defdsl migration
  "Define a migration entity"
  [name docstr parents & ops]
  (let [parents (if (empty? parents)
                    [:chimera/root-migration]
                    parents)
        parent-txdata (for [p parents]
                          {:chimera.migration/name p})
        op-eids (map :db/id ops)
        ops (map (fn [op next-eid]
                   (if next-eid
                     (assoc op :chimera.migration.operation/next next-eid)
                     op))
              ops (concat (drop 1 op-eids) [nil]))
        txdata (conj ops
                 {:db/id (cfg/tempid -42)
                  :chimera.migration/name name
                  :chimera.migration/doc docstr
                  :chimera.migration/parents parent-txdata
                  :chimera.migration/operation (first op-eids)})]
    (script/transact txdata)))


(s/fdef extend-type
  :args (s/cat :supertype ::type-name
               :subtype ::type-name))

(defdsl extend-type
  "Create an extend-type operation for a migration"
  [supertype subtype]
  {:db/id (cfg/tempid)
   :chimera.migration.operation/operation-type :chimera/extend-type
   :chimera.migration.operation.extend-type/supertype supertype
   :chimera.migration.operation.extend-type/subtype subtype})

(defmulti primitive-type
  "Open predicate for primitive type mappings."
  identity :default ::default)

(defmethod primitive-type ::default [kw]
  ({:boolean :chimera.primitive/boolean
    :string  :chimera.primitive/string
    :keyword :chimera.primitive/keyword
    :long    :chimera.primitive/long
    :double  :chimera.primitive/double
    :bigdec  :chimera.primitive/bigdec
    :bigint  :chimera.primitive/bigint
    :instant :chimera.primitive/instant
    :uuid    :chimera.primitive/uuid
    :bytes   :chimera.primitive/bytes} kw))

(s/def ::primitive-type primitive-type)

(s/def ::attr-dsl-feature
  (s/alt
    :min (s/cat :kw #{:min} :val number?)
    :max (s/cat :kw #{:max} :val number?)
    :prim ::primitive-type
    :ref (s/cat :kw #{:ref} :val ::type-name)
    :component (s/cat :kw #{:ref} :val ::type-name)
    :index #{:index}
    :key #{:key}))

(s/def ::attr-dsl-features (s/+ ::attr-dsl-feature))

(s/fdef attr
  :args (s/cat :name ::attribute-name
               :type ::type-name
               :features ::attr-dsl-features))

(defn- update-feature
  "Update an attr txdata map with a conformed DSL feature"
  [attr-map [feature {val :val :as v}]]
  (let [m (case feature
            :min {:chimera.attribute/min-cardinality val}
            :max {:chimera.attribute/max-cardinality val}
            :prim {:chimera.attribute/range (primitive-type v)}
            :ref {:chimera.attribute/range val}
            :component {:chimera.attribute/range val
                        :chimera.attribute/domain val}
            :index {:chimera.attribute/indexed true}
            :key {:chimera.attribute/key true})]
    (merge attr-map m)))

(defdsl attr
  "Create an add-attribute operation for a migration"
  [name type & features]
  (let [features (s/conform ::attr-dsl-features features)
        attr-txdata {:chimera.attribute/name name
                     :chimera.attribute/domain type}
        attr-txdata (reduce update-feature attr-txdata features)
        attr-txdata (update attr-txdata :chimera.attribute/min-cardinality
                      #(if (nil? %) 0 %))]
    {:db/id (cfg/tempid)
     :chimera.migration.operation/operation-type :chimera/add-attribute
     :chimera.migration.operation.add-attribute/attr attr-txdata}))