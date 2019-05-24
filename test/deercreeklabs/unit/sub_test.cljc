(ns deercreeklabs.unit.sub-test
  (:require
   [clojure.test :refer [are deftest is]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt]))

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(l/def-int-map-schema users-schema
  user-schema)

(l/def-record-schema msg-schema
  [:user user-schema]
  [:text l/string-schema])

(l/def-record-schema sys-state-schema
  [:msgs (l/array-schema msg-schema)]
  [:users users-schema])

(l/def-record-schema foo-schema
  [:a l/int-schema])

(l/def-record-schema bar-schema
  [:a l/string-schema])

(l/def-record-schema foo-foos-schema
  [:foo foo-schema]
  [:foos (l/map-schema foo-schema)])

(l/def-map-schema map-of-fbs-schema
  (l/union-schema [foo-schema bar-schema]))

(deftest test-sub-schemas-complex
  (let [ret (->> (l/sub-schemas sys-state-schema)
                 (map u/edn-schema)
                 (map u/edn-schema->name-kw)
                 (set))
        expected #{:array
                   :string
                   :deercreeklabs.unit.sub-test/msg
                   :deercreeklabs.unit.sub-test/sys-state
                   :deercreeklabs.unit.sub-test/user
                   :deercreeklabs.unit.sub-test/users}]
    (is (= expected ret))))

(deftest test-sub-schemas
  (let [ret (->> (l/sub-schemas lt/add-to-cart-rsp-schema)
                 (map u/edn-schema)
                 (map u/edn-schema->name-kw)
                 (set))
        expected #{:deercreeklabs.unit.lancaster-test/add-to-cart-rsp
                   :deercreeklabs.unit.lancaster-test/a-fixed
                   :int
                   :deercreeklabs.unit.lancaster-test/why
                   :bytes
                   :deercreeklabs.unit.lancaster-test/add-to-cart-req}]
    (is (= expected ret))))

(deftest test-sub-schemas-union
  (let [ret (->> (l/sub-schemas lt/person-or-dog-schema)
                 (map u/edn-schema)
                 (map u/edn-schema->name-kw)
                 (set))
        expected #{:union
                   :deercreeklabs.unit.lancaster-test/dog
                   :deercreeklabs.unit.lancaster-test/person}]
    (is (= expected ret))))

(deftest test-sub-schemas-repeated-schemas
  (let [ret (->> (l/sub-schemas foo-foos-schema)
                 (map u/edn-schema)
                 (map u/edn-schema->name-kw)
                 (set))
        expected #{:int
                   :map
                   :deercreeklabs.unit.sub-test/foo
                   :deercreeklabs.unit.sub-test/foo-foos}]
    (is (= expected ret))))

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
    (is (= :deercreeklabs.unit.lancaster-test/why ret))))

(deftest test-schema-at-path-map-in-rec-1
  (let [path [:name-to-age]
        ret (-> (l/schema-at-path lt/rec-w-map-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :map ret))))

(deftest test-schema-at-path-map-in-rec-2
  (let [path [:name-to-age "Bo"]
        ret (-> (l/schema-at-path lt/rec-w-map-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :int ret))))

(deftest test-schema-at-path-int-map
  (let [path []
        ret (-> (l/schema-at-path lt/sku-to-qty-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :deercreeklabs.unit.lancaster-test/sku-to-qty ret))))

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
