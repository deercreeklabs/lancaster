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

(l/def-record-schema unq-rec-a-schema
  {:key-ns-type :none}
  [:a l/int-schema])

(l/def-record-schema unq-rec-b-schema
  {:key-ns-type :none}
  [:b l/int-schema])

(def maybe-unq-rec-schema (l/maybe unq-rec-a-schema))

(l/def-union-schema union-of-two-unq-reqs
  unq-rec-a-schema unq-rec-b-schema)

(l/def-union-schema union-of-two-unq-reqs-reversed
  unq-rec-b-schema unq-rec-a-schema)

(deftest test-forgot-ns-keys-in-union
  (let [data {:name "Cally"
              :age 24}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Can't serialize ambiguous record with union schema"
         (l/serialize lt/person-or-dog-schema data)))))

(deftest test-map-and-multi-rec-union
  (let [schema (l/union-schema [(l/map-schema l/int-schema)
                                lt/person-schema lt/dog-schema])]
    (is (lt/round-trip? schema {"foo" 1}))
    (is (lt/round-trip? schema #:person{:name "Chad" :age 18}))
    (is (lt/round-trip? schema #:dog{:name "Bowser" :owner "Chad"}))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Can't serialize ambiguous record with union schema"
         (l/serialize schema {:name "Chad" :age 18})))))

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
  (let [data {:a 42}
        data-w-meta (with-meta data {:short-name :unq-rec-a})
        encoded (l/serialize union-of-two-unq-reqs data-w-meta)
        decoded (l/deserialize-same union-of-two-unq-reqs encoded)
        output-meta {:branch-index 0
                     :fq-name :deercreeklabs.unit.ns-keys-test/unq-rec-a
                     :short-name :unq-rec-a}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Can't serialize ambiguous record with union schema"
         (l/serialize union-of-two-unq-reqs data)))
    (is (= data decoded))
    (is (= output-meta (meta decoded)))))

(deftest test-unqualified-record-in-union-resolution
  (let [data {:a 42}
        data-w-meta (with-meta data {:short-name :unq-rec-a})
        encoded (l/serialize union-of-two-unq-reqs data-w-meta)
        decoded (l/deserialize union-of-two-unq-reqs-reversed
                               union-of-two-unq-reqs encoded)
        output-meta {:branch-index 1
                     :fq-name :deercreeklabs.unit.ns-keys-test/unq-rec-a
                     :short-name :unq-rec-a}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Can't serialize ambiguous record with union schema"
         (l/serialize union-of-two-unq-reqs data)))
    (is (= data decoded))
    (is (= output-meta (meta decoded)))))

(deftest test-maybe-unq-record
  (is (lt/round-trip? maybe-unq-rec-schema {:a 42})))
