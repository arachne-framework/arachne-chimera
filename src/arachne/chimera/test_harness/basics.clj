(ns arachne.chimera.test-harness.basics
  (:require [arachne.chimera :as chimera]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.chimera.test-harness.common :as common]
            [com.stuartsierra.component :as component]
            [clojure.test :as test :refer [testing is]])
  (:import [java.util UUID Date]
           [arachne ArachneException]))

(defn bad-operations
  [adapter-dsl-fn modules]
  (let [cfg (core/build-config modules `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]
    (is (thrown-with-msg? ArachneException #"not supported"
                          (chimera/operate adapter :no.such/operation [])))
    (is (thrown-with-msg? ArachneException #"Unknown dispatch"
                          (chimera/operate adapter :test.operation/foo [])))
    (is (thrown-with-msg? ArachneException #"did not conform "
                          (chimera/operate adapter :chimera.operation/initialize-migrations false)))))

(defn simple-crud
  [adapter-dsl-fn modules]
  (let [cfg (core/build-config modules `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (let [james (UUID/randomUUID)
          mary (UUID/randomUUID)]

      (chimera/ensure-migrations rt [:arachne/id :test/adapter])

      (testing "testing basic put/get"
        (chimera/operate adapter :chimera.operation/put {:test.person/id james
                                                         :test.person/name "James"})
        (chimera/operate adapter :chimera.operation/put {:test.person/id mary
                                                         :test.person/name "Mary"})
        (is (= {:test.person/id james, :test.person/name "James"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))
        (is (= {:test.person/id mary, :test.person/name "Mary"}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id mary))))
        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id (UUID/randomUUID)))))
        (is (thrown-with-msg? ArachneException #"already"
                              (chimera/operate adapter :chimera.operation/put {:test.person/id james
                                                                               :test.person/name "James"})))
        (is (thrown-with-msg? ArachneException #"requires a key"
                              (chimera/operate adapter :chimera.operation/put {:test.person/name "Elizabeth"}))))

      (testing "testing update operation"
        (let [t1 (java.util.Date.)
              t2 (java.util.Date.)]

          (chimera/operate adapter :chimera.operation/update {:test.person/id james
                                                              :test.person/dob t1})

          (is (= {:test.person/id james,
                  :test.person/name "James"
                  :test.person/dob t1}
                 (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

          (is (thrown-with-msg? ArachneException #"does not exist"
                                (chimera/operate adapter :chimera.operation/update {:test.person/id (UUID/randomUUID)
                                                                                    :test.person/dob t1})))

          (is (thrown-with-msg? ArachneException #"requires a key"
                                (chimera/operate adapter :chimera.operation/update {:test.person/dob t1})))


          (chimera/operate adapter :chimera.operation/update {:test.person/id james
                                                              :test.person/name "Jimmy"
                                                              :test.person/dob t2})

          (is (= {:test.person/id james,
                  :test.person/name "Jimmy"
                  :test.person/dob t2}
                 (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))))

      (testing "delete entity"

        (chimera/operate adapter :chimera.operation/delete-entity (chimera/lookup :test.person/id james))

        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (is (thrown-with-msg? ArachneException #"does not exist"
                              (chimera/operate adapter :chimera.operation/delete-entity
                                               (chimera/lookup :test.person/id james))))))))

(defn delete-attributes
  [adapter-dsl-fn modules]
  (let [cfg (core/build-config modules `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (let [james (UUID/randomUUID)
          original-map {:test.person/id james
                        :test.person/name "James"
                        :test.person/dob (Date.)}]

      (testing "Testing delete attr"
        (chimera/operate adapter :chimera.operation/put original-map)

        (chimera/operate adapter :chimera.operation/delete [(chimera/lookup :test.person/id james)
                                                            :test.person/name "Jimmy-jim"])

        (is (= original-map (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (chimera/operate adapter :chimera.operation/delete [(chimera/lookup :test.person/id james)
                                                            :test.person/name "James"])

        (is (= (dissoc original-map :test.person/name)
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (chimera/operate adapter :chimera.operation/delete [(chimera/lookup :test.person/id james)
                                                            :test.person/dob])

        (is (= {:test.person/id james}
               (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james)))))

      (testing "delete attr cannot delete key attributes"

        (is (thrown-with-msg? ArachneException #"Cannot delete key attribute"
              (chimera/operate adapter :chimera.operation/delete [(chimera/lookup :test.person/id james)
                                                                  :test.person/id])
              ))

        ))))

(defn exercise-all
  [adapter-dsl-fn modules]
  (bad-operations adapter-dsl-fn modules)
  (simple-crud adapter-dsl-fn modules)
  (delete-attributes adapter-dsl-fn modules))

