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

(l/def-fixed-schema some-bytes-schema 2)

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested :int]
  [:qty-added :int]
  [:current-qty :int]
  [:req add-to-cart-req-schema {:sku 10 :qty-requested 1}]
  [:the-reason-why why-schema :stock]
  [:data some-bytes-schema]
  [:other-data some-bytes-schema (ba/byte-array [2 8])])

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
    (is (= expected-cpf (l/get-canonical-parsing-form
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
  (is (= {:namespace "deercreeklabs.lancaster-test"
          :name :some-bytes
          :type :fixed
          :size 2}
         (l/get-edn-schema some-bytes-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.SomeBytes\",\"type\":"
              "\"fixed\",\"size\":2}")
         (l/get-canonical-parsing-form some-bytes-schema)))
  (is (= "sSmUfOioIiPbljVrNw83MQ=="
         (ba/byte-array->b64 (l/get-fingerprint128 some-bytes-schema)))))

(deftest test-def-fixed-schema-serdes
  (let [data (ba/byte-array [12 24])
        encoded (l/serialize some-bytes-schema data)
        decoded (l/deserialize some-bytes-schema some-bytes-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         data encoded))
    (is (ba/equivalent-byte-arrays? data decoded))
    #?(:clj
       (let [decoded-native (l/deserialize some-bytes-schema some-bytes-schema
                                           encoded true)]
         (is (= (SomeBytes. data) decoded-native))))))

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
                     :name :some-bytes
                     :type :fixed
                     :size 2}
                    :default (ba/byte-array [])}
                   {:name :other-data
                    :type
                    {:namespace "deercreeklabs.lancaster-test"
                     :name :some-bytes
                     :type :fixed
                     :size 2}
                    :default (ba/byte-array [2 8])}]}
        actual (l/get-edn-schema add-to-cart-rsp-schema)]
    (is (ba/equivalent-byte-arrays? (get-in expected [:fields 5 :default])
                                    (get-in actual [:fields 5 :default])))
    (is (ba/equivalent-byte-arrays? (get-in expected [:fields 6 :default])
                                    (get-in actual [:fields 6 :default])))
    (is (= (sanitize-byte-arrays expected) (sanitize-byte-arrays actual))))
  (is (=
       (str
        "{\"name\":\"deercreeklabs.lancaster_test.AddToCartRsp\",\"type\":"
        "\"record\",\"fields\":[{\"name\":\"qtyRequested\",\"type\":\"int\"},"
        "{\"name\":\"qtyAdded\",\"type\":\"int\"},{\"name\":\"currentQty\","
        "\"type\":\"int\"},{\"name\":\"req\",\"type\":{\"name\":"
        "\"deercreeklabs.lancaster_test.AddToCartReq\",\"type\":\"record\","
        "\"fields\":[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
        "\"qtyRequested\",\"type\":\"int\"}]}},{\"name\":\"theReasonWhy\","
        "\"type\":{\"name\":\"deercreeklabs.lancaster_test.Why\",\"type\":"
        "\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}},{\"name\":"
        "\"data\",\"type\":{\"name\":\"deercreeklabs.lancaster_test.SomeBytes\""
        ",\"type\":\"fixed\",\"size\":2}},{\"name\":\"otherData\",\"type\":"
        "\"deercreeklabs.lancaster_test.SomeBytes\"}]}")
       (l/get-canonical-parsing-form add-to-cart-rsp-schema)))
  (is (= "csp/yHGPm6CJwRSSq6R2JQ=="
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
    (is (= "9gEBAfYB9gECQkN7ew==" (ba/byte-array->b64 encoded)))
    (is (= (sanitize-byte-arrays expected)
           (sanitize-byte-arrays decoded)))))
