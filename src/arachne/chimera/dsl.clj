(ns arachne.chimera.dsl
  (:require [clojure.spec :as s]
            [arachne.chimera.specs :as cs]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.config :as cfg]
            [arachne.core.config.script :as script :refer [defdsl]]))

(defdsl migration
  "Define a migration entity with the specified name, docstring, set of parents and operations."
  (s/cat :name :chimera.migration/name
         :docstr string?
         :parents (s/coll-of :chimera.migration/name)
         :ops (s/+ ::cs/operation-txmap))
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
    :ref (s/cat :kw #{:ref} :val :chimera.type/name)
    :component (s/cat :kw #{:component} :val :chimera.type/name)
    :index #{:index}
    :key #{:key}))

(s/def ::attr-dsl-features (s/+ ::attr-dsl-feature))

(defn- update-feature
  "Update an attr txdata map with a conformed DSL feature"
  [attr-map [feature {val :val :as v}]]
  (let [m (case feature
            :min {:chimera.attribute/min-cardinality val}
            :max {:chimera.attribute/max-cardinality val}
            :prim {:chimera.attribute/range (primitive-type v)}
            :ref {:chimera.attribute/range val}
            :component {:chimera.attribute/range val
                        :chimera.attribute/component true}
            :index {:chimera.attribute/indexed true}
            :key {:chimera.attribute/key true})]
    (merge attr-map m)))

(defdsl attr
  "Create an add-attribute operation for a migration"
  (s/cat :name :chimera.attribute/name
    :type :chimera.type/name
    :features ::attr-dsl-features)
  [name type & features]
  (let [features (s/conform ::attr-dsl-features features)
        attr-txdata {:chimera.attribute/name name
                     :chimera.attribute/domain type}
        attr-txdata (reduce update-feature attr-txdata features)
        attr-txdata (update attr-txdata :chimera.attribute/min-cardinality
                      #(if (nil? %) 0 %))]
    {:db/id (cfg/tempid)
     :chimera.migration.operation/type :chimera.operation/add-attribute
     :chimera.operation.add-attribute/attr attr-txdata}))

