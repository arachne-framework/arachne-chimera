(ns arachne.chimera.specs
  (:require [clojure.spec :as s]))

;; Structure of operation entity map txdata, for the config (eventually can be generated from model...)
(s/def ::operation-txmap (s/keys :req [:chimera.migration.operation/type]
                                 :opt [:chimera.migration.operation/next]))


;; Structure of type entity map txdata, for the config (eventually can be generated from model...)
(s/def ::type-txmap (s/keys :req [:chimera.type/name]
                            :opt [:chimera.type/supertypes]))


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
(s/def :chimera.type/supertypes (s/coll-of :chimera.type/name))

(s/def :chimera.attribute/name (s/and keyword? namespace))
(s/def :chimera.attribute/domain :chimera.type/name)
(s/def :chimera.attribute/range :chimera.type/name)
(s/def :chimera.attribute/min-cardinality int?)
(s/def :chimera.attribute/max-cardinality int?)
(s/def :chimera.attribute/key boolean)
(s/def :chimera.attribute/indexed boolean)

