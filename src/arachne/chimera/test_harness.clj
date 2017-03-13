(ns arachne.chimera.test-harness
  (:require [arachne.chimera :as chimera]
            [arachne.chimera.test-harness.batch :as batch]
            [arachne.chimera.test-harness.basics :as basics]
            [clojure.test :as test :refer [testing is]])
  (:import [java.util UUID]))

(defn exercise-all
  "Given a DSL function that takes a migration and defines an adapter, exercise Chimera's
   full adapater test harness."
  [adapter-dsl-fn]
  (basics/exercise-all adapter-dsl-fn)
  (batch/exercise adapter-dsl-fn))



