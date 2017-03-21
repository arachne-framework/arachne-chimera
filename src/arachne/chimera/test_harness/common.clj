(ns arachne.chimera.test-harness.common
  (:require [arachne.core.dsl :as a]
            [arachne.chimera.dsl :as ch]))

(defn config
  "DSL function to build a test config"
  [dob-min-card test-adapter-fn]

  (ch/migration :test/m1
                "Migration to set up schema for example-based tests"
                []
                (ch/attr :test.person/id :test/Person :key :uuid :min 1 :max 1)
                (ch/attr :test.person/name :test/Person :string :min 1 :max 1))
  (ch/migration :test/m2
                "Migration to set up schema for example-based tests"
                [:test/m1]
                (ch/attr :test.person/dob :test/Person :instant :min dob-min-card :max 1)
                (ch/attr :test.person/friends :test/Person :ref :test/Person :min 0)
                (ch/attr :test.person/best-friend :test/Person :ref :test/Person :min 0 :max 1)

                (ch/attr :test.person/addresses :test/Person :component :test/Address :min 0)
                (ch/attr :test.person/primary-address :test/Person :component :test/Address :min 0 :max 1)

                (ch/attr :test.address/street :test/Address :string :min 1 :max 1)
                )

  (a/id :test/adapter (test-adapter-fn :test/m2))

  (a/id :test/rt (a/runtime [:test/adapter])))
