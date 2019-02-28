(ns deercreeklabs.unit.edn-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [are deftest is use-fixtures]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt]
   [schema.core :as s :include-macros true]))


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
