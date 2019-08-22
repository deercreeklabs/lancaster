(ns deercreeklabs.unit.sub-test
  (:require
   [clojure.test :refer [are deftest is]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt]))

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(l/def-record-schema msg-schema
  [:user user-schema]
  [:text l/string-schema])

(l/def-record-schema sys-state-schema
  [:msgs (l/array-schema msg-schema)]
  [:users (l/array-schema user-schema)])

(l/def-record-schema foo-schema
  [:foo/a :required l/int-schema])

(l/def-record-schema bar-schema
  [:bar/a :required l/string-schema])

(l/def-record-schema foo-foos-schema
  [:foo foo-schema]
  [:foos (l/map-schema foo-schema)])

(l/def-map-schema map-of-fbs-schema
  (l/union-schema [foo-schema bar-schema]))

(l/def-record-schema person-schema
  [:name l/string-schema]
  [:age l/int-schema]
  [:children (l/array-schema ::person)])

(def mary
  {:name "Mary"
   :age 11
   :children []})

(def bob
  {:name "Bob"
   :age 10
   :children []})

(def ralph
  {:name "Ralph"
   :age 100
   :children [bob mary]})

(deftest test-schema-at-path-nested-recs
  (let [path [:req :sku]
        ret (-> (l/schema-at-path lt/add-to-cart-rsp-schema path)
                (u/edn-schema))]
    (is (= :int ret))))

(deftest test-schema-at-path-rec
  (let [path [:req]
        ret (-> (l/schema-at-path lt/add-to-cart-rsp-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :deercreeklabs.unit.lancaster-test/add-to-cart-req ret))))

(deftest test-schema-at-path-rec-root
  (let [path []
        ret (-> (l/schema-at-path lt/add-to-cart-rsp-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :deercreeklabs.unit.lancaster-test/add-to-cart-rsp ret))))

(deftest test-schema-at-path-array-in-rec-1
  (let [path [:names]
        ret (-> (l/schema-at-path lt/rec-w-array-and-enum-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :array ret))))

(deftest test-schema-at-path-array-in-rec-2
  (let [path [:names 1234]
        ret (-> (l/schema-at-path lt/rec-w-array-and-enum-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :string ret))))

(deftest test-schema-at-path-enum-in-rec
  (let [path [:why]
        ret (-> (l/schema-at-path lt/rec-w-array-and-enum-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :union ret))))

(deftest test-schema-at-path-map-in-rec-1
  (let [path [:name-to-age]
        ret (-> (l/schema-at-path lt/rec-w-map-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :union ret))))

(deftest test-schema-at-path-map-in-rec-2
  (let [path [:name-to-age "Bo"]
        ret (-> (l/schema-at-path lt/rec-w-map-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :int ret))))

(deftest test-schema-at-path-union-root
  (let [path []
        ret (-> (l/schema-at-path map-of-fbs-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :map ret))))

(deftest test-schema-at-path-union-one-arg
  (let [path ["a"]
        ret (-> (l/schema-at-path map-of-fbs-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :union ret))))

(deftest test-schema-at-path-union-two-args-int
  (let [path ["a" :foo/a]
        ret (-> (l/schema-at-path map-of-fbs-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :int ret))))

(deftest test-schema-at-path-union-two-args-str
  (let [path ["a" :bar/a]
        ret (-> (l/schema-at-path map-of-fbs-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :string ret))))

(deftest test-schema-at-path-repeated-schema
  (let [path [:foos "x"]
        ret (-> (l/schema-at-path foo-foos-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :deercreeklabs.unit.sub-test/foo ret))))

(deftest test-schema-at-path-nil-path
  (let [ret (-> (l/schema-at-path foo-foos-schema nil)
                (l/edn))]
    (is (= (l/edn foo-foos-schema) ret))))

(deftest test-schema-at-path-recursive
  (let [schema (l/schema-at-path person-schema [:children])]
    (is (lt/round-trip? schema [ralph]))))

(deftest test-schema-at-path-recursive-deep
  (let [schema (l/schema-at-path person-schema
                                 [:children 0 :children 1 :children])]
    (is (lt/round-trip? schema [ralph]))))
