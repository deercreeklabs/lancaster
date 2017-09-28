(ns deercreeklabs.lancaster-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [cljs.core.async.macros :as ca])))

(u/configure-logging)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Unit tests

(l/def-record-schema add-to-cart-req-schema
  [:sku :int]
  [:qty :int 0])

(l/def-enum-schema why-schema
  :all :stock :limit)

(l/def-fixed-schema some-bytes-schema 16)

;; (l/def-record-schema add-to-cart-rsp-schema
;;   [:qty-requested :int]
;;   [:qty-added :int]
;;   [:current-qty :int]
;;   [:req add-to-cart-req-schema]
;;   [:why why-schema :all]
;;   [:data some-bytes-schema])

;; (def a-union
;;   (l/avro-union [add-to-cart-req-schema add-to-cart-rsp-schema]))

;; (def array-schema (l/avro-array some-bytes-schema))

(deftest test-record-schema
  (let [expected-cpf (str "{\"name\":\"deercreeklabs.lancaster_test."
                          "AddToCartReq\",\"type\":\"record\",\"fields\":"
                          "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                          "\"qty\",\"type\":\"int\"}]}")
        expected-edn-schema {:namespace "deercreeklabs.lancaster_test",
                             :name "AddToCartReq",
                             :type :record,
                             :fields
                             [{:name "sku", :type :int, :default -1}
                              {:name "qty", :type :int, :default 0}]}]
    (is (= "55c9GhZaAtoAeeM4KoXnLA=="
           (ba/byte-array->b64 (l/get-fingerprint128 add-to-cart-req-schema))))
    (is (= expected-edn-schema (l/get-edn-schema add-to-cart-req-schema)))
    (is (= expected-cpf (l/get-canonical-parsing-form
                         add-to-cart-req-schema)))))

(deftest test-def-record-schema-serdes
  (let [data {:sku 123
              :qty 5}
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
             qty (.getQty ^AddToCartReq decoded-native)]
         (is (= 123 sku))
         (is (= 5 qty))))))

(deftest test-def-enum-schema
  (is (= {:namespace "deercreeklabs.lancaster_test"
          :name "Why"
          :type :enum
          :symbols [:all :stock :limit]}
         (l/get-edn-schema why-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.Why\",\"type\":"
              "\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}")
         (l/get-canonical-parsing-form why-schema)))
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
  (is (= {:namespace "deercreeklabs.lancaster_test"
          :name "SomeBytes"
          :type :fixed
          :size 16}
         (l/get-edn-schema some-bytes-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.SomeBytes\",\"type\":"
              "\"fixed\",\"size\":16}")
         (l/get-canonical-parsing-form some-bytes-schema)))
  (is (= "iKjdzS827GIqTVhEPFMJ3w=="
         (ba/byte-array->b64 (l/get-fingerprint128 some-bytes-schema)))))

(deftest test-def-fixed-schema-serdes
  (let [data (ba/byte-array (range 16))
        encoded (l/serialize some-bytes-schema data)
        decoded (l/deserialize some-bytes-schema some-bytes-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]) encoded))
    (is (ba/equivalent-byte-arrays? data decoded))
    #?(:clj
       (let [decoded-native (l/deserialize some-bytes-schema some-bytes-schema
                                           encoded true)]
         (is (= (SomeBytes. data) decoded-native))))))

;; (deftest test-nested-schemas
;;   (is (= {:namespace "deercreeklabs.lancaster_test"
;;           :name "AddToCartRsp"
;;           :type :record
;;           :fields
;;           [{:name "qtyRequested" :type :int :default -1}
;;            {:name "qtyAdded" :type :int :default -1}
;;            {:name "currentQty" :type :int :default -1}
;;            {:name "req"
;;             :type "AddToCartReq"
;;             :default {"sku" -1 "qty" 0}}
;;            {:name "why" :type "Why" :default "ALL"}
;;            {:name "data" :type "SomeBytes" :default ""}]}
;;          add-to-cart-rsp-schema)))

;; (deftest test-avro-union
;;   (is (= ["AddToCartReq" "AddToCartRsp"]
;;          a-union)))

;; (deftest test-avro-array
;;   (is (= {:type :array :values "SomeBytes"}
;;          array-schema)))
#_
(deftest test-gen-classes
  (let [ns (find-ns 'deercreeklabs.lancaster-test)
        ret (l/gen-classes ns "gen-java")]
    (is (= true ret))))
