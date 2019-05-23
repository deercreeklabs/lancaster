(ns deercreeklabs.unit.edn-test
  (:require
   [clojure.test :refer [are deftest is]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.unit.lancaster-test :as lt]))

(deftest test-edn->schema
  (are [edn] (= edn (-> edn l/edn->schema l/edn))
    :string
    :int
    [:null :int :string]
    {:name ::test-fixed
     :type :fixed
     :size 16}
    {:name ::test-enum
     :key-ns-type :short
     :type :enum
     :symbols [:a :b :c]}
    {:name ::test-rec
     :type :record
     :key-ns-type :short
     :fields [{:name :a
               :type :int
               :default -1}
              {:name :b
               :type :string
               :default ""}]}) )

(deftest test-name-kw
  (let [cases [[l/string-schema :string]
               [l/int-schema :int]
               [lt/why-schema :deercreeklabs.unit.lancaster-test/why]
               [lt/a-fixed-schema :deercreeklabs.unit.lancaster-test/a-fixed]
               [lt/ages-schema :map]
               [lt/simple-array-schema :array]
               [lt/rec-w-array-and-enum-schema
                :deercreeklabs.unit.lancaster-test/rec-w-array-and-enum]
               [lt/maybe-int-schema :union]]]
    (doseq [[sch expected] cases]
      (let [edn (l/edn sch)
            ret (l/name-kw sch)]
        (is (= [edn expected] [edn ret]))))))
