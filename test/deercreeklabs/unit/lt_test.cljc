(ns deercreeklabs.unit.lt-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt]
   [schema.core :as s :include-macros true])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

(l/def-int-map-schema sku-to-qty-schema
  l/int-schema)

(def sku-to-qty-v2-schema (l/int-map-schema ::sku-to-qty l/long-schema))

(l/def-int-map-schema im-schema
  l/string-schema)

(l/def-fixed-map-schema fm-schema
  16 l/string-schema)

(l/def-union-schema im-or-fm-schema
  im-schema
  fm-schema)

(l/def-union-schema lt-union-schema
  l/keyword-schema
  l/string-schema
  l/int-schema)

(deftest test-int-map-schema
  (is (= {:name :deercreeklabs.unit.lt-test/sku-to-qty
          :type :record
          :fields [{:name :deercreeklabs-unit-lt-test-sku-to-qty/ks
                    :type {:type :array
                           :items :int}
                    :default []}
                   {:name :deercreeklabs-unit-lt-test-sku-to-qty/vs
                    :type {:type :array
                           :items :int}
                    :default []}]}
         (u/strip-lt-attrs (l/edn sku-to-qty-schema))))
  #?(:clj (is (lt/fp-matches? sku-to-qty-schema)))
  (is (= (str
          "{\"name\":\"deercreeklabs.unit.lt_test.SkuToQty\",\"type\":"
          "\"record\",\"fields\":[{\"name\":"
          "\"deercreeklabsUnitLtTestSkuToQtyKs\",\"type\":{\"type\":\"array\","
          "\"items\":\"int\"}},{\"name\":\"deercreeklabsUnitLtTestSkuToQtyVs\","
          "\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}")
         (l/pcf sku-to-qty-schema)))
  (is (= (str
          "{\"name\":\"deercreeklabs.unit.lt_test.SkuToQty\",\"fields\":"
          "[{\"name\":\"deercreeklabsUnitLtTestSkuToQtyKs\",\"type\":{\"type\":"
          "\"array\",\"items\":\"int\"},\"default\":[]},{\"name\":"
          "\"deercreeklabsUnitLtTestSkuToQtyVs\",\"type\":{\"type\":\"array\","
          "\"items\":\"int\"},\"default\":[]}],\"type\":\"record\","
          "\"logicalType\":\"int-map\"}")
         (l/json sku-to-qty-schema)))
  (is (= "-7668463894600081969"
         (u/long->str (l/fingerprint64 sku-to-qty-schema)))))

(deftest test-embedded-int-map-pcf
  (let [fms (l/int-map-schema ::my-map l/int-schema)
        rs (l/record-schema :r [[:fm fms]])]
    (is (= (str
            "{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"fm\","
            "\"type\":[\"null\",{\"name\":\"deercreeklabs.unit.lt_test.MyMap\""
            ",\"type\":\"record\",\"fields\":[{\"name\":"
            "\"deercreeklabsUnitLtTestMyMapKs\",\"type\":{\"type\":\"array\","
            "\"items\":\"int\"}},{\"name\":\"deercreeklabsUnitLtTestMyMapVs\","
            "\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}]}]}")
           (l/pcf rs)))))

(deftest test-int-map-schema-serdes
  (let [data {123 10
              456 100
              789 2}
        encoded (l/serialize sku-to-qty-schema data)
        decoded (l/deserialize-same sku-to-qty-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 -10 1 -112 7 -86 12 0 6 20 -56 1 4 0])
         encoded))
    (is (= data decoded))))

(deftest test-int-map-serdes-empty-map
  (let [data {}
        encoded (l/serialize sku-to-qty-schema data)
        decoded (l/deserialize-same sku-to-qty-schema encoded)]
    (is (= data decoded))))

(deftest test-maybe-int-map
  (let [int-map-schema (l/int-map-schema ::im l/int-schema)
        maybe-schema (l/maybe int-map-schema)
        data1 {1 1}
        data2 nil]
    (is (lt/round-trip? maybe-schema data1))
    (is (lt/round-trip? maybe-schema data2))))

(deftest test-bad-fixed-map-size
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Second argument to fixed-map-schema must be a positive integer"
       (l/fixed-map-schema ::x -1 l/string-schema))))

(deftest test-int-map-evolution
  (let [data {123 10
              456 100
              789 2}
        encoded (l/serialize sku-to-qty-schema data)
        decoded (l/deserialize sku-to-qty-v2-schema
                               sku-to-qty-schema encoded)]
    (is (= data decoded))))

(deftest test-schema-at-path-int-map
  (let [path [1]
        ret (-> (l/schema-at-path sku-to-qty-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :int ret))))

;; The new child-schema based schema at path doesn't use a key when getting
;; the child schema for an array or map (or logical-type semantic equevalent)
;; since all the values have the same type. So when you have a path for
;; schema-at-path you can put literally anything in that position of the path.
; (deftest test-schema-at-path-int-map-bad-key
;   (is (thrown-with-msg?
;        #?(:clj ExceptionInfo :cljs js/Error)
;        #"Key `a` is not a valid key for logical type `int-map`"
;        (l/schema-at-path sku-to-qty-schema ["a"]))))

(deftest test-schema-at-path-int-map-empty-path
  (let [path []
        ret (-> (l/schema-at-path sku-to-qty-schema path)
                (u/edn-schema)
                (u/edn-schema->name-kw))]
    (is (= :deercreeklabs.unit.lt-test/sku-to-qty ret))))

(deftest test-flex-map-union
  (let [data1 {(ba/byte-array (range 16)) "name1"}
        data2 {1 "str2"}
        enc1 (l/serialize im-or-fm-schema data1)
        enc2 (l/serialize im-or-fm-schema data2)
        rt1 (l/deserialize-same im-or-fm-schema enc1)
        rt2 (l/deserialize-same im-or-fm-schema enc2)]
    (is (ba/equivalent-byte-arrays? (ffirst data1) (ffirst rt1)))
    (is (= (nfirst data1) (nfirst rt1)))
    (is (= data2 rt2))))

(deftest test-keyword-schema
  (let [data1 :a-simple-kw
        data2 ::a-namespaced-kw]
    (is (lt/round-trip? l/keyword-schema data1))
    (is (lt/round-trip? l/keyword-schema data2))))

(deftest test-keyword-union-schema
  (let [data1 :a-simple-kw
        data2 ::a-namespaced-kw
        data3 123]
    (is (lt/round-trip? lt-union-schema data1))
    (is (lt/round-trip? lt-union-schema data2))
    (is (lt/round-trip? lt-union-schema data3))))

(deftest test-lt-schema-at-path
  (let [sch (l/map-schema lt-union-schema)
        child-sch (l/schema-at-path sch ["a"])
        data :kw1]
    (is (lt/round-trip? child-sch data))))

(deftest test-name-kw-lt
  (let [sch (l/record-schema ::sch
                             [[:foo-a l/keyword-schema]
                              [:bar-b l/keyword-schema]])
        data1 {:foo-a :a
               :bar-b :an-ns/b}]
    (is (lt/round-trip? sch data1))))

(deftest test-default-data-lt
  (let [sch (l/int-map-schema ::test l/string-schema)
        data (l/default-data sch)]
    (is (= {} data))))

(deftest test-lt-in-record
  (let [user-schema (l/record-schema ::user
                                     [[:name l/string-schema]
                                      [:nickname l/string-schema]])
        users-schema (l/int-map-schema ::users user-schema)
        state-schema (l/record-schema ::state
                                      [[:users users-schema]])
        data {:users {1 {:name "Bo" :nickname "B"}}}
        encoded (l/serialize state-schema data)
        decoded (l/deserialize-same state-schema encoded)]
    (is (= data decoded))))

(deftest test-lt-in-record-from-json
  (let [schema (l/int-map-schema ::im l/string-schema)
        rt-schema (l/json->schema (l/pcf schema))
        data {1 "yyy"}
        encoded (l/serialize schema data)
        decoded (l/deserialize schema rt-schema encoded)]
    (is (= data decoded))))
