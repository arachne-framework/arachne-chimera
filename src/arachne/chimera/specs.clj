(ns arachne.chimera.specs
  (:require [clojure.spec.alpha :as s]
            [arachne.core.util :as u]))

(s/def :chimera/adapter (u/lazy-satisfies? arachne.chimera.adapter/Adapter))

(defn lookup? [obj]
  (instance? (resolve 'arachne.chimera.Lookup) obj))

(s/def :chimera/lookup lookup?)

;; Structure of operation entity map txdata, for the config (eventually can be generated from model...)
(s/def ::operation-txmap (s/keys :req [:chimera.migration.operation/type]
                                 :opt [:chimera.migration.operation/next]))


;; Structure of type entity map txdata, for the config (eventually can be generated from model...)
(s/def ::type-txmap (s/keys :req [:chimera.type/name]))


;; Structure of attribute entity map txdata, for the config (eventually can be generated from model...)
(s/def ::attribute-txmap (s/keys :req [:chimera.attribute/name
                                       :chimera.attribute/domain
                                       :chimera.attribute/range
                                       :chimera.attribute/min-cardinality]
                                 :opt [:chimera.attribute/max-cardinality
                                       :chimera.attribute/key
                                       :chimera.attribute/indexed]))


(s/def :chimera.migration/name (s/and keyword? namespace))

(s/def :chimera.migration.operation/type (s/and keyword? namespace))
(s/def :chimera.migration.operation/next ::operation-txmap)

(s/def :chimera.type/name (s/and keyword? namespace))

(s/def :chimera.attribute/name (s/and keyword? namespace))
(s/def :chimera.attribute/domain :chimera.type/name)
(s/def :chimera.attribute/range :chimera.type/name)
(s/def :chimera.attribute/min-cardinality int?)
(s/def :chimera.attribute/max-cardinality int?)
(s/def :chimera.attribute/key boolean)
(s/def :chimera.attribute/indexed boolean)

(s/def :chimera/operation-type (s/and keyword? namespace))
(s/def :chimera/operation-payload any?)

(s/def :chimera.primitive/boolean boolean?)
(s/def :chimera.primitive/string string?)
(s/def :chimera.primitive/keyword keyword?)
(s/def :chimera.primitive/long integer?)
(s/def :chimera.primitive/double float?)
(s/def :chimera.primitive/bigdec decimal?)
(s/def :chimera.primitive/bigint #(or (instance? BigInteger %)
                                      (instance? clojure.lang.BigInt %)))
(s/def :chimera.primitive/instant #(instance? java.util.Date %))
(s/def :chimera.primitive/uuid uuid?)
(s/def :chimera.primitive/bytes bytes?)

(s/def :chimera/primitive (s/or
                            :boolean :chimera.primitive/boolean
                            :string  :chimera.primitive/string
                            :keyword :chimera.primitive/keyword
                            :long    :chimera.primitive/long
                            :double  :chimera.primitive/double
                            :bigdec  :chimera.primitive/bigdec
                            :bigint  :chimera.primitive/bigint
                            :instant :chimera.primitive/instant
                            :uuid    :chimera.primitive/uuid
                            :bytes   :chimera.primitive/bytes))

(s/def :chimera/entity-map-with-components
  (s/map-of :chimera.attribute/name (s/or :primitive :chimera/primitive
                                          :ref :chimera/lookup
                                          :component :chimera/entity-map-with-components
                                          :coll (s/coll-of (s/or :primitive :chimera/primitive
                                                                 :ref :chimera/lookup
                                                                 :component :chimera/entity-map-with-components)
                                                            :kind set?))
            :min-count 1))


(s/def :chimera/entity-map
  (s/map-of :chimera.attribute/name (s/or :primitive :chimera/primitive
                                          :ref :chimera/lookup
                                          :coll (s/coll-of (s/or :primitive :chimera/primitive
                                                                 :ref :chimera/lookup)
                                                           :kind set?))
            :min-count 1))


