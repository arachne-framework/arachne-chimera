(ns arachne.chimera.dsl
  (:refer-clojure :exclude [extend-type])
  (:require [clojure.spec :as s]
            [arachne.chimera.specs :as cs]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.dsl.specs :as core-specs]
            [arachne.core.config :as cfg]
            [arachne.core.config.init :as script :refer [defdsl]]))


(s/fdef migration
  :args (s/cat :name :chimera.migration/name
               :docstr string?
               :parents (s/coll-of :chimera.migration/name)
               :ops (s/+ ::cs/operation-txmap)))

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
  :args (s/cat :supertype :chimera.type/name
               :subtype :chimera.type/name))

(defdsl extend-type
  "Create an extend-type operation for a migration"
  [supertype subtype]
  {:db/id (cfg/tempid)
   :chimera.migration.operation/type :chimera.operation/extend-type
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
    :ref (s/cat :kw #{:ref} :val :chimera.type/name)
    :component (s/cat :kw #{:ref} :val :chimera.type/name)
    :index #{:index}
    :key #{:key}))

(s/def ::attr-dsl-features (s/+ ::attr-dsl-feature))

(s/fdef attr
  :args (s/cat :name :chimera.attribute/name
               :type :chimera.type/name
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
     :chimera.migration.operation/type :chimera.operation/add-attribute
     :chimera.migration.operation.add-attribute/attr attr-txdata}))

(s/fdef config-adapter
  :args (s/cat :arachne-id ::core-specs/id))

(defdsl config-adapter
  "Define a config adapter component in the configuration."
  [arachne-id]
  (let [tempid (cfg/tempid)]
    (cfg/resolve-tempid
      (script/transact [{:db/id tempid
                         :arachne/id arachne-id
                         :chimera.adapter/type :arachne.chimera.adapter/config}])
      tempid)))