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

      #_(testing "can't set cardinality-many refs using single value"
        (is (thrown-with-msg? ArachneException #"cardinality"
              (chimera/operate adapter :chimera.operation/update
                               {:test.person/id mary
                                :test.person/friends (chimera/lookup :test.person/id james)}))))



      #_(chimera/operate adapter :chimera.operation/put {:test.person/id mary
                                                       :test.person/name "Mary"})
      #_(chimera/operate adapter :chimera.operation/put {:test.person/id elizabeth
                                                       :test.person/name "Mary"})


      )))

(defn exercise-all
  [adapter-dsl-fn]
  (basic-ref-operations adapter-dsl-fn)

  )

#_(test/deftest run
    (basic-ref-operations arachne.chimera.test-adapter/test-adapter))



