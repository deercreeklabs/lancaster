(ns deercreeklabs.unit.sub-test
  (:require
   [clojure.test :refer [are deftest is]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt]))

(deftest test-sub-schemas
  (let [subs (l/sub-schemas lt/add-to-cart-rsp-schema)
        edn-schemas (map u/edn-schema subs)
        schema-names (map u/edn-schema->name-kw edn-schemas)
        expected [:deercreeklabs.unit.lancaster-test/add-to-cart-rsp
                  :deercreeklabs.unit.lancaster-test/a-fixed
                  :int
                  :deercreeklabs.unit.lancaster-test/why
                  :bytes
                  :deercreeklabs.unit.lancaster-test/add-to-cart-req]]
    (is (= expected schema-names))))

(deftest test-sub-schemas-union
  (let [subs (l/sub-schemas lt/person-or-dog-schema)
        edn-schemas (map u/edn-schema subs)
        schema-names (map u/edn-schema->name-kw edn-schemas)
        expected [:union
                  :deercreeklabs.unit.lancaster-test/person
                  :deercreeklabs.unit.lancaster-test/dog]]
    (is (= expected schema-names))))

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
