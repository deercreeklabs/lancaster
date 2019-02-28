(ns deercreeklabs.unit.ns-keys-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-fq-ns-rec
  (let [schema (l/record-schema ::lt/rec {:key-ns-type :fq}
                                [[:a l/int-schema]])]
    (is (lt/round-trip? schema {:deercreeklabs.unit.lancaster-test.rec/a 1}))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Missing namespace on key `:a`"
         (l/serialize schema {:a 1})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key"
         (l/serialize schema {::lt/a 1})))))

(deftest test-unq-ns-rec
  (let [schema (l/record-schema ::lt/rec {:key-ns-type :none}
                                [[:a l/int-schema]])]
    (is (lt/round-trip? schema {:a 1}))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:a`"
         (l/serialize schema {:deercreeklabs.unit.lancaster-test.rec/a 1})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:a`"
         (l/serialize schema {::lt/a 1})))))

(deftest test-fq-ns-enum
  (let [schema (l/enum-schema ::lt/enum-test {:key-ns-type :fq}
                              [:a :b :c])]
    (is (lt/round-trip? schema :deercreeklabs.unit.lancaster-test.enum-test/a))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Enum data `:a` is missing the proper namespace qualifier"
         (l/serialize schema :a)))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"is not one of the symbols of this enum"
         (l/serialize schema ::lt/a)))))

(deftest test-unq-ns-enum
  (let [schema (l/enum-schema ::lt/enum-test {:key-ns-type :none}
                              [:a :b :c])]
    (is (lt/round-trip? schema :a))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"`:deercreeklabs.unit.lancaster-test/a` is not one of"
         (l/serialize schema ::lt/a)))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"`:deercreeklabs.unit.ns-keys-test/a` is not one of the symbols"
         (l/serialize schema ::a)))))

(deftest test-unqualified-record-in-union
  (let [unq-rec-schema (l/record-schema ::rec {:key-ns-type :none}
                                        [[:a l/int-schema]])]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Records in unions must have namespace-qualified keys"
         (l/union-schema [unq-rec-schema l/int-schema])))))
