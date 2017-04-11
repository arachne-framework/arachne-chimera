(ns arachne.chimera.test-harness.batch
  (:require [arachne.chimera :as chimera]
            [arachne.core :as core]
            [arachne.core.config :as cfg]
            [arachne.core.runtime :as rt]
            [arachne.chimera.test-harness.common :as common]
            [com.stuartsierra.component :as component]
            [clojure.test :as test :refer [testing is]])
  (:import [java.util UUID]
           [arachne ArachneException]))

(defn- atomic-batches?
  "Test if an adapter supports atomic batches"
  [adapter]
  (cfg/q (:arachne/config adapter)
         '[:find ?a
           :where
           [?a :chimera.adapter/capabilities ?cap]
           [?cap :chimera.adapter.capability/operation ?op]
           [?op :chimera.operation/type :chimera.operation/batch]
           [?ap :chimera.adapter.capability/atomic? true]]))

(defn exercise
  [adapter-dsl-fn modules]
  (let [cfg (core/build-config modules `(common/config 0 ~adapter-dsl-fn))
        rt (core/runtime cfg :test/rt)
        rt (component/start rt)]
    (chimera/ensure-migrations rt [:arachne/id :test/adapter])
    (let [adapter (rt/lookup rt [:arachne/id :test/adapter])
          james (UUID/randomUUID)
          mary (UUID/randomUUID)]

      (testing "put and get"
        (chimera/operate adapter :chimera.operation/batch
                         [[:chimera.operation/put {:test.person/id james
                                                   :test.person/name "James"}]
                          [:chimera.operation/put {:test.person/id mary
                                                   :test.person/name "Mary"}]])

        (is (= {:test.person/id james
                :test.person/name "James"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (is (= {:test.person/id mary
                :test.person/name "Mary"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id mary)))))

      (testing "update and delete"
        (chimera/operate adapter :chimera.operation/batch
                         [[:chimera.operation/put {:test.person/id james
                                                   :test.person/name "Jimmy"}]
                          [:chimera.operation/delete-entity (chimera/lookup :test.person/id mary)]])

        (is (= {:test.person/id james
                :test.person/name "Jimmy"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id mary)))))

      (when (atomic-batches? adapter)
        (testing "transactionality"
          (let [edward (UUID/randomUUID)]
            (is (thrown-with-msg? ArachneException #"does not exist"
                                  (chimera/operate adapter :chimera.operation/batch
                                                   [[:chimera.operation/put {:test.person/id edward
                                                                             :test.person/name "Edward"}]
                                                    [:chimera.operation/delete-entity (chimera/lookup :test.person/id (UUID/randomUUID))]])))
            (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id edward))))))))))

