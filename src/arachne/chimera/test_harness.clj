(ns arachne.chimera.test-harness
  (:require [arachne.chimera :as chimera]
            [arachne.chimera.test-harness.batch :as batch]
            [arachne.chimera.test-harness.basics :as basics]
            [arachne.chimera.test-harness.refs :as refs]
            [clojure.test :as test :refer [testing is]])
  (:import [java.util UUID]))

(defn exercise-all
  "Given a DSL function that takes a migration and defines an adapter, exercise Chimera's
   full adapater test harness."
  [adapter-dsl-fn modules]
  (basics/exercise-all adapter-dsl-fn modules)
  (batch/exercise adapter-dsl-fn modules)
  (refs/exercise-all adapter-dsl-fn modules))



