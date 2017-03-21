(ns arachne.chimera.test-harness.refs
  (:require [arachne.chimera :as chimera]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.chimera.test-harness.common :as common]
            [com.stuartsierra.component :as component]
            [clojure.test :as test :refer [testing is]])
  (:import [java.util UUID Date]
           [arachne ArachneException]))

(defn basic-ref-operations
  [adapter-dsl-fn]
  (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
                               `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (let [james (UUID/randomUUID)
          mary (UUID/randomUUID)
          elizabeth (UUID/randomUUID)]

      (testing "cannot create ref to nonexistent entity"
        (is (thrown-with-msg? ArachneException #"does not exist"
                              (chimera/operate adapter :chimera.operation/put
                                               {:test.person/id james
                                                :test.person/name "James"
                                                :test.person/friends #{(chimera/lookup [:test.person/id mary])}})))
        (chimera/operate adapter :chimera.operation/put {:test.person/id james
                                                         :test.person/name "James"})
        (is (thrown-with-msg? ArachneException #"does not exist"
                              (chimera/operate adapter :chimera.operation/update
                                               {:test.person/id james
                                                :test.person/friends #{(chimera/lookup [:test.person/id mary])}}))))

      (testing "can create refs to elements in the same batch"
        (is (chimera/operate adapter :chimera.operation/batch
                             [[:chimera.operation/put {:test.person/id mary
                                                       :test.person/name "Mary"}]
                              [:chimera.operation/update {:test.person/id james
                                                          :test.person/friends #{(chimera/lookup [:test.person/id mary])}}]])))

      (testing "can't set cardinality-many refs using single value"
        (is (thrown-with-msg? ArachneException #"be a set"
              (chimera/operate adapter :chimera.operation/update
                               {:test.person/id mary
                                :test.person/friends (chimera/lookup :test.person/id james)}))))

      (testing "can't set cardinality-one refs using a set"
        (is (thrown-with-msg? ArachneException #"be a single value"
                              (chimera/operate adapter :chimera.operation/update
                                               {:test.person/id mary
                                                :test.person/best-friend #{(chimera/lookup :test.person/id james)}}))))

      (testing "ref attrs are removed when target is removed (card many)"
        (chimera/operate adapter :chimera.operation/batch
          [[:chimera.operation/put {:test.person/id elizabeth
                                    :test.person/name "Elizabeth"}]
           [:chimera.operation/update {:test.person/id james
                                       :test.person/friends #{(chimera/lookup :test.person/id elizabeth)}}]])
        (is (= 2 (count (:test.person/friends (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))))
        (chimera/operate adapter :chimera.operation/delete-entity (chimera/lookup :test.person/id mary))
        (is (= 1 (count (:test.person/friends (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james)))))))

      (testing "ref attrs are removed when target is removed (card one)"
        (chimera/operate adapter :chimera.operation/batch
          [[:chimera.operation/update {:test.person/id james
                                       :test.person/best-friend (chimera/lookup :test.person/id elizabeth)}]])
        (is (:test.person/best-friend (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))
        (chimera/operate adapter :chimera.operation/delete-entity (chimera/lookup :test.person/id elizabeth))
        (is (not (:test.person/best-friend (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))))

      )))

(defn component-operations
  [adapter-dsl-fn]
  (let [cfg (core/build-config [:org.arachne-framework/arachne-chimera]
              `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (let [james (UUID/randomUUID)
          mary (UUID/randomUUID)
          elizabeth (UUID/randomUUID)]

      (testing "Components can be set as nested entities"
        (chimera/operate adapter :chimera.operation/put
          {:test.person/id james
           :test.person/primary-address {:test.address/street "Buckingham"}})

        #_(is (= {:test.person/id james
                :test.person/primary-address {:test.address/street "Buckingham"}}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))



        )



      )))

(comment

  (require '[clojure.spec :as s])

  (def james (UUID/randomUUID))
  (def emap {:test.person/id james
             :test.person/primary-address {:test.address/street "Buckingham"}})

  (s/conform :chimera/entity-map emap)


  )


(defn exercise-all
  [adapter-dsl-fn]
  (basic-ref-operations adapter-dsl-fn)
  (component-operations adapter-dsl-fn))

(comment

 (require '[arachne.chimera.test-adapter])

 (test/deftest run
   (exercise-all arachne.chimera.test-adapter/test-adapter))

 )



