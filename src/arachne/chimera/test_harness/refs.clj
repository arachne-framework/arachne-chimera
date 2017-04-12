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
  [adapter-dsl-fn modules]
  (let [cfg (core/build-config modules `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (chimera/ensure-migrations rt [:arachne/id :test/adapter])

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
                                                         :test.person/name "James"}))

      (testing "can create refs to elements in the same batch"
        (is (nil? (chimera/operate adapter :chimera.operation/batch
                    [[:chimera.operation/put {:test.person/id mary
                                              :test.person/name "Mary"}]
                     [:chimera.operation/put {:test.person/id james
                                              :test.person/friends #{(chimera/lookup [:test.person/id mary])}}]]))))

      (testing "can't set cardinality-many refs using single value"
        (is (thrown-with-msg? ArachneException #"be a set"
              (chimera/operate adapter :chimera.operation/put
                               {:test.person/id mary
                                :test.person/friends (chimera/lookup :test.person/id james)}))))

      (testing "can't set cardinality-one refs using a set"
        (is (thrown-with-msg? ArachneException #"be a single value"
                              (chimera/operate adapter :chimera.operation/put
                                               {:test.person/id mary
                                                :test.person/best-friend #{(chimera/lookup :test.person/id james)}}))))

      (testing "ref attrs are removed when target is removed (card many)"
        (chimera/operate adapter :chimera.operation/batch
          [[:chimera.operation/put {:test.person/id elizabeth
                                    :test.person/name "Elizabeth"}]
           [:chimera.operation/put {:test.person/id james
                                       :test.person/friends #{(chimera/lookup :test.person/id elizabeth)}}]])
        (is (= 2 (count (:test.person/friends (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))))
        (chimera/operate adapter :chimera.operation/delete-entity (chimera/lookup :test.person/id mary))
        (is (= 1 (count (:test.person/friends (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james)))))))

      (testing "ref attrs are removed when target is removed (card one)"
        (chimera/operate adapter :chimera.operation/batch
          [[:chimera.operation/put {:test.person/id james
                                       :test.person/best-friend (chimera/lookup :test.person/id elizabeth)}]])
        (is (:test.person/best-friend (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))
        (chimera/operate adapter :chimera.operation/delete-entity (chimera/lookup :test.person/id elizabeth))
        (is (not (:test.person/best-friend (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))))

      )))

(defn component-operations
  [adapter-dsl-fn modules]
  (let [cfg (core/build-config modules `(common/config 0 ~adapter-dsl-fn))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]

    (chimera/ensure-migrations rt [:arachne/id :test/adapter])

    (let [james (UUID/randomUUID)
          detail1 (UUID/randomUUID)
          detail2 (UUID/randomUUID)
          address1 (UUID/randomUUID)
          address2 (UUID/randomUUID)
          address3 (UUID/randomUUID)]

      (testing "Components are returned as nested maps"
        (chimera/operate adapter :chimera.operation/batch
          [[:chimera.operation/put {:test.address-detail/id detail1
                                    :test.address-detail/note "Some notes"}]
           [:chimera.operation/put {:test.address-detail/id detail2
                                    :test.address-detail/note "Some other notes"}]
           [:chimera.operation/put {:test.address/id address1
                                    :test.address/street "Street 1"
                                    :test.address/detail #{(chimera/lookup :test.address-detail/id detail1)}}]
           [:chimera.operation/put {:test.address/id address2
                                    :test.address/street "Street 2"
                                    :test.address/detail #{(chimera/lookup :test.address-detail/id detail2)}}]
           [:chimera.operation/put {:test.address/id address3
                                    :test.address/street "Street 3"}]
           [:chimera.operation/put {:test.person/id james
                                    :test.person/primary-address (chimera/lookup :test.address/id address1)
                                    :test.person/addresses #{(chimera/lookup :test.address/id address2)
                                                             (chimera/lookup :test.address/id address3)}}]])

        (is (= {:test.person/id james
                :test.person/primary-address {:test.address/id address1
                                              :test.address/street "Street 1"
                                              :test.address/detail #{{:test.address-detail/id detail1
                                                                      :test.address-detail/note "Some notes"}}}
                :test.person/addresses #{{:test.address/id address2
                                          :test.address/street "Street 2"
                                          :test.address/detail #{{:test.address-detail/id detail2
                                                                  :test.address-detail/note "Some other notes"}}}
                                         {:test.address/id address3
                                          :test.address/street "Street 3"}}}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (is (= {:test.address/id address1
                :test.address/street "Street 1"
                :test.address/detail #{{:test.address-detail/id detail1
                                        :test.address-detail/note "Some notes"}}}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address/id address1))))

        (is (= {:test.address/id address2
                :test.address/street "Street 2"
                :test.address/detail #{{:test.address-detail/id detail2
                                        :test.address-detail/note "Some other notes"}}}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address/id address2))))

        (is (= {:test.address-detail/id detail1
                :test.address-detail/note "Some notes"}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address-detail/id detail1))))

        (is (= {:test.address-detail/id detail2
                :test.address-detail/note "Some other notes"}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address-detail/id detail2)))))


      (testing "Deletes are recursive"
        (chimera/operate adapter :chimera.operation/delete [(chimera/lookup :test.person/id james) :test.person/primary-address])

        (is (= {:test.person/id james
                :test.person/addresses #{{:test.address/id address2
                                          :test.address/street "Street 2"
                                          :test.address/detail #{{:test.address-detail/id detail2
                                                                  :test.address-detail/note "Some other notes"}}}
                                         {:test.address/id address3
                                          :test.address/street "Street 3"}}}
              (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))

        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address/id address1))))
        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address-detail/id detail1))))

        (chimera/operate adapter :chimera.operation/delete-entity (chimera/lookup :test.person/id james))

        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.person/id james))))
        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address/id address2))))
        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address/id address3))))
        (is (nil? (chimera/operate adapter :chimera.operation/get (chimera/lookup :test.address-detail/id detail2))))))))

(defn exercise-all
  [adapter-dsl-fn modules]
  (basic-ref-operations adapter-dsl-fn modules)
  (component-operations adapter-dsl-fn modules))
