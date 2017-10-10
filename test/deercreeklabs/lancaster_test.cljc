(ns deercreeklabs.lancaster-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [cljs.core.async.macros :as ca])))

(u/configure-logging)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Unit tests

(l/def-record-schema add-to-cart-req-schema
  [:sku :int]
  [:qty-requested :int 0])

(l/def-enum-schema why-schema
  :all :stock :limit)

(l/def-fixed-schema a-fixed-schema 2)

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested :int]
  [:qty-added :int]
  [:current-qty :int]
  [:req add-to-cart-req-schema {:sku 10 :qty-requested 1}]
  [:the-reason-why why-schema :stock]
  [:data a-fixed-schema (ba/byte-array [2 8])]
  [:other-data :bytes #_(ba/byte-array [1 2 3 4])])

(deftest test-record-schema
  (let [expected-cpf (str "{\"name\":\"deercreeklabs.lancaster_test."
                          "AddToCartReq\",\"type\":\"record\",\"fields\":"
                          "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                          "\"qtyRequested\",\"type\":\"int\"}]}")
        expected-edn-schema {:namespace "deercreeklabs.lancaster-test",
                             :name :add-to-cart-req,
                             :type :record,
                             :fields
                             [{:name :sku, :type :int, :default -1}
                              {:name :qty-requested, :type :int, :default 0}]}]
    (is (= "u7RfQa4gIP8+iNcQxGO+Ng=="
           (ba/byte-array->b64 (l/get-fingerprint128 add-to-cart-req-schema))))
    (is (= expected-edn-schema (l/get-edn-schema add-to-cart-req-schema)))
    (is (= expected-cpf (l/get-parsing-canonical-form
                         add-to-cart-req-schema)))))

(deftest test-def-record-schema-serdes
  (let [data {:sku 123
              :qty-requested 5}
        encoded (l/serialize add-to-cart-req-schema data)
        decoded (l/deserialize add-to-cart-req-schema add-to-cart-req-schema
                               encoded)]
    (is (= "9gEK" (ba/byte-array->b64 encoded)))
    (is (= data decoded))
    #?(:clj
       (let [decoded-native (l/deserialize add-to-cart-req-schema
                                           add-to-cart-req-schema
                                           encoded true)
             sku (.getSku ^AddToCartReq decoded-native)
             qty (.getQtyRequested ^AddToCartReq decoded-native)]
         (is (= 123 sku))
         (is (= 5 qty))))))

(deftest test-def-enum-schema
  (is (= {:namespace "deercreeklabs.lancaster-test"
          :name :why
          :type :enum
          :symbols [:all :stock :limit]}
         (l/get-edn-schema why-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.Why\",\"type\":"
              "\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}")
         (l/get-parsing-canonical-form why-schema)))
  (is (= "PRicwxPrcK1IC1N+1axUGg=="
         (ba/byte-array->b64 (l/get-fingerprint128 why-schema)))))

(deftest test-def-enum-schema-serdes
  (let [data :stock
        encoded (l/serialize why-schema data)
        decoded (l/deserialize why-schema why-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [2]) encoded))
    (is (= data decoded))
    #?(:clj
       (let [decoded-native (l/deserialize why-schema why-schema encoded true)]
         (is (= Why/STOCK decoded-native))))))

(deftest test-def-fixed-schema
  (is (= {:namespace "deercreeklabs.lancaster-test"
          :name :a-fixed
          :type :fixed
          :size 2}
         (l/get-edn-schema a-fixed-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.AFixed\",\"type\":"
              "\"fixed\",\"size\":2}")
         (l/get-parsing-canonical-form a-fixed-schema)))
  (is (= "6rfEbxq6V1fSCxTa1lLKXg=="
         (ba/byte-array->b64 (l/get-fingerprint128 a-fixed-schema)))))

(deftest test-def-fixed-schema-serdes
  (let [data (ba/byte-array [12 24])
        encoded (l/serialize a-fixed-schema data)
        decoded (l/deserialize a-fixed-schema a-fixed-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         data encoded))
    (is (ba/equivalent-byte-arrays? data decoded))
    #?(:clj
       (let [decoded-native (l/deserialize a-fixed-schema a-fixed-schema
                                           encoded true)]
         (is (= (AFixed. data) decoded-native))))))

(defn sanitize-byte-arrays
  "Replaces byte arrays with nil so they can be compared by clojure.test/is."
  [edn-schema]
  (clojure.walk/postwalk #(when-not (ba/byte-array? %) %) edn-schema))

(deftest test-nested-record-schema
  (let [expected {:namespace "deercreeklabs.lancaster-test"
                  :name :add-to-cart-rsp
                  :type :record
                  :fields
                  [{:name :qty-requested :type :int :default -1}
                   {:name :qty-added :type :int :default -1}
                   {:name :current-qty :type :int :default -1}
                   {:name :req
                    :type
                    {:namespace "deercreeklabs.lancaster-test"
                     :name :add-to-cart-req
                     :type :record
                     :fields
                     [{:name :sku :type :int :default -1}
                      {:name :qty-requested :type :int :default 0}]}
                    :default {:sku 10 :qty-requested 1}}
                   {:name :the-reason-why
                    :type
                    {:namespace "deercreeklabs.lancaster-test"
                     :name :why
                     :type :enum
                     :symbols [:all :stock :limit]}
                    :default :stock}
                   {:name :data
                    :type
                    {:namespace "deercreeklabs.lancaster-test"
                     :name :a-fixed
                     :type :fixed
                     :size 2}
                    :default (ba/byte-array [2 8])}
                   {:name :other-data
                    :type :bytes
                    :default (ba/byte-array [])}]}
        actual (l/get-edn-schema add-to-cart-rsp-schema)]
    (is (ba/equivalent-byte-arrays? (get-in expected [:fields 5 :default])
                                    (get-in actual [:fields 5 :default])))
    (is (= (sanitize-byte-arrays expected) (sanitize-byte-arrays actual))))
  (is (= "GdzZ8c+EGQ/cFI/XgtXXIA=="
         (ba/byte-array->b64 (l/get-fingerprint128 add-to-cart-rsp-schema)))))

(deftest test-nested-record-serdes
  (let [data {:qty-requested 123
              :req {:sku 123 :qty-requested 123}
              :data (ba/byte-array [66 67])
              :other-data (ba/byte-array [123 123])}
        expected (assoc data
                        :qty-added -1
                        :current-qty -1
                        :the-reason-why :stock)
        encoded (l/serialize add-to-cart-rsp-schema data)
        decoded (l/deserialize add-to-cart-rsp-schema add-to-cart-rsp-schema
                               encoded)]
    (is (= "9gEBAfYB9gECQkMEe3s=" (ba/byte-array->b64 encoded)))
    (is (= (sanitize-byte-arrays expected)
           (sanitize-byte-arrays decoded)))))

(deftest test-null-schema
  (let [data nil
        encoded (l/serialize l/null-schema data)
        decoded (l/deserialize l/null-schema l/null-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array []) encoded))
    (is (= data decoded))))

(deftest test-boolean-schema
  (let [data true
        encoded (l/serialize l/boolean-schema data)
        decoded (l/deserialize l/boolean-schema l/boolean-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [1]) encoded))
    (is (= data decoded))))

(deftest test-int-schema
  (let [data 7890
        encoded (l/serialize l/int-schema data)
        decoded (l/deserialize l/int-schema l/int-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-92 123]) encoded))
    (is (= data decoded))))

(deftest test-long-schema
  (let [data 9223372036854775807
        encoded (l/serialize l/long-schema data)
        decoded (l/deserialize l/long-schema l/long-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [-2 -1 -1 -1 -1 -1 -1 -1 -1 1])
         encoded))
    (is (= data decoded))))

(deftest test-float-schema
  (let [data (float 3.14159)
        encoded (l/serialize l/float-schema data)
        decoded (l/deserialize l/float-schema l/float-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-48 15 73 64]) encoded))
    (is (= data decoded))))

(deftest test-double-schema
  (let [data (double 3.14159265359)
        encoded (l/serialize l/double-schema data)
        decoded (l/deserialize l/double-schema l/double-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-22 46 68 84 -5 33 9 64])
                                    encoded))
    (is (= data decoded))))

(deftest test-bytes-schema
  (let [data (ba/byte-array [1 1 2 3 5 8 13 21])
        encoded (l/serialize l/bytes-schema data)
        decoded (l/deserialize l/bytes-schema l/bytes-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [16 1 1 2 3 5 8 13 21])
                                    encoded))
    (is (ba/equivalent-byte-arrays? data decoded))))

(deftest test-string-schema
  (let [data "Hello world!"
        encoded (l/serialize l/string-schema data)
        decoded (l/deserialize l/string-schema l/string-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [24 72 101 108 108 111 32 119 111 114 108 100 33])
         encoded))
    (is (= data decoded))))


;; TODO: Test all primitives alone and as fields with and without defaults
