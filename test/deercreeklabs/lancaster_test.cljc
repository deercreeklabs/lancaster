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

(defn deserialize-same
  "Deserialize with the same reader and writer schemas. Use for testing only."
  ([schema encoded]
   (deserialize-same schema encoded false))
  ([schema encoded return-java?]
   (l/deserialize schema (l/get-json-schema schema) encoded return-java?)))

(l/def-record-schema add-to-cart-req-schema
  [:sku :int]
  [:qty-requested :int 0])

(l/def-record-schema add-to-cart-req-v2-schema
  [:username :string]
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
  [:data a-fixed-schema (ba/byte-array [77 88])]
  [:other-data :bytes])

(l/def-array-schema rsps-schema
  add-to-cart-rsp-schema)

(l/def-array-schema simple-array-schema
  :string)

(l/def-map-schema ages-schema
  :int)

(l/def-map-schema nested-map-schema
  add-to-cart-rsp-schema)

(l/def-union-schema union-schema
  :int add-to-cart-req-schema a-fixed-schema)

(l/def-record-schema tree-schema
  [:value :int]
  [:right l/nil-or-recur-schema]
  [:left l/nil-or-recur-schema])

(deftest test-record-schema
  (let [expected-cpf (str "{\"name\":\"deercreeklabs.lancaster_test."
                          "AddToCartReq\",\"type\":\"record\",\"fields\":"
                          "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                          "\"qtyRequested\",\"type\":\"int\"}]}")
        expected-edn-schema {:namespace "deercreeklabs.lancaster-test"
                             :name :add-to-cart-req
                             :type :record
                             :fields
                             [{:name :sku :type :int :default -1}
                              {:name :qty-requested :type :int :default 0}]}]
    (is (= "u7RfQa4gIP8+iNcQxGO+Ng=="
           (ba/byte-array->b64 (l/get-fingerprint128 add-to-cart-req-schema))))
    (is (= expected-edn-schema (l/get-edn-schema add-to-cart-req-schema)))
    (is (= expected-cpf (l/get-parsing-canonical-form
                         add-to-cart-req-schema)))))

(deftest test-def-record-schema-serdes
  (let [data {:sku 123
              :qty-requested 5}
        encoded (l/serialize add-to-cart-req-schema data)
        decoded (deserialize-same add-to-cart-req-schema encoded)]
    (is (= "9gEK" (ba/byte-array->b64 encoded)))
    (is (= data decoded))
    #?(:clj
       (let [decoded-native (deserialize-same
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
        decoded (deserialize-same why-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [2]) encoded))
    (is (= data decoded))
    #?(:clj
       (let [decoded-native (deserialize-same why-schema encoded true)]
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
        decoded (deserialize-same a-fixed-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         data encoded))
    (is (ba/equivalent-byte-arrays? data decoded))
    #?(:clj
       (let [decoded-native (deserialize-same a-fixed-schema
                                           encoded true)]
         (is (= (AFixed. data) decoded-native))))))

(defn xf-byte-arrays
  [edn-schema]
  (clojure.walk/postwalk #(if (ba/byte-array? %)
                            (u/byte-array->byte-str %)
                            %)
                         edn-schema))

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
                    :default "MX"}
                   {:name :other-data
                    :type :bytes
                    :default ""}]}]
    (is (= expected (l/get-edn-schema add-to-cart-rsp-schema))))
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
        decoded (deserialize-same add-to-cart-rsp-schema
                               encoded)]
    (is (= "9gEBAfYB9gECQkMEe3s=" (ba/byte-array->b64 encoded)))
    (is (= (xf-byte-arrays expected)
           (xf-byte-arrays decoded)))))

(deftest test-null-schema
  (let [data nil
        encoded (l/serialize l/null-schema data)
        decoded (deserialize-same l/null-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array []) encoded))
    (is (= data decoded))))

(deftest test-boolean-schema
  (let [data true
        encoded (l/serialize l/boolean-schema data)
        decoded (deserialize-same l/boolean-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [1]) encoded))
    (is (= data decoded))))

(deftest test-int-schema
  (let [data 7890
        encoded (l/serialize l/int-schema data)
        decoded (deserialize-same l/int-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-92 123]) encoded))
    (is (= data decoded))))

(deftest test-long-schema
  (let [data 9223372036854775807
        encoded (l/serialize l/long-schema data)
        decoded (deserialize-same l/long-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [-2 -1 -1 -1 -1 -1 -1 -1 -1 1])
         encoded))
    (is (= data decoded))))

(deftest test-float-schema
  (let [data (float 3.14159)
        encoded (l/serialize l/float-schema data)
        decoded (deserialize-same l/float-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-48 15 73 64]) encoded))
    (is (= data decoded))))

(deftest test-double-schema
  (let [data (double 3.14159265359)
        encoded (l/serialize l/double-schema data)
        decoded (deserialize-same l/double-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-22 46 68 84 -5 33 9 64])
                                    encoded))
    (is (= data decoded))))

(deftest test-bytes-schema
  (let [data (ba/byte-array [1 1 2 3 5 8 13 21])
        encoded (l/serialize l/bytes-schema data)
        decoded (deserialize-same l/bytes-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [16 1 1 2 3 5 8 13 21])
                                    encoded))
    (is (ba/equivalent-byte-arrays? data decoded))))

(deftest test-string-schema
  (let [data "Hello world!"
        encoded (l/serialize l/string-schema data)
        decoded (deserialize-same l/string-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [24 72 101 108 108 111 32 119 111 114 108 100 33])
         encoded))
    (is (= data decoded))))

(deftest test-def-map-schema
  (is (= {:type :map :values :int}
         (l/get-edn-schema ages-schema)))
  (is (= "{\"type\":\"map\",\"values\":\"int\"}"
         (l/get-parsing-canonical-form ages-schema)))
  (is (= "Ld3h1A8PSHaljEYqsZx5ag=="
         (ba/byte-array->b64 (l/get-fingerprint128 ages-schema)))))

(deftest test-map-schema-serdes
  (let [data {"Alice" 50
              "Bob" 55
              "Chad" 89}
        encoded (l/serialize ages-schema data)
        decoded (deserialize-same ages-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 10 65 108 105 99 101 100 6 66 111 98 110 8
                         67 104 97 100 -78 1 0])
         encoded))
    (is (= data decoded))
    #?(:clj
       (let [decoded-native (deserialize-same ages-schema
                                           encoded true)]
         (is (= java.util.HashMap
                (class decoded-native)))))))

(deftest test-def-array-schema
  (is (= {:type :array :items :string}
         (l/get-edn-schema simple-array-schema)))
  (is (= "{\"type\":\"array\",\"items\":\"string\"}"
         (l/get-parsing-canonical-form simple-array-schema)))
  (is (= "ldhJq9T0nUDg7+XR3Z2rYw=="
         (ba/byte-array->b64 (l/get-fingerprint128 simple-array-schema)))))

(deftest test-array-schema-serdes
  (let [names ["Ferdinand" "Omar" "Lin"]
        encoded (l/serialize simple-array-schema names)
        decoded (deserialize-same simple-array-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 18 70 101 114 100 105 110 97 110 100 8 79
                         109 97 114 6 76 105 110 0])
         encoded))
    (is (= names decoded))
    #?(:clj
       (let [decoded-native (deserialize-same
                                           simple-array-schema
                                           encoded true)]
         (is (= org.apache.avro.generic.GenericData$Array
                (class decoded-native)))))))

(deftest test-nested-array-schema
  (is (= {:type :array
          :items
          {:namespace "deercreeklabs.lancaster-test"
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
             :default "MX"}
            {:name :other-data :type :bytes :default ""}]}}
         (l/get-edn-schema rsps-schema)))
  (is (= "+BBySe1XenuRgfqEKzDfXQ=="
         (ba/byte-array->b64 (l/get-fingerprint128 rsps-schema)))))

(deftest test-nested-array-schema-serdes
  (let [data [{:qty-requested 123
               :qty-added 4
               :current-qty 10
               :req {:sku 123 :qty-requested 123}
               :the-reason-why :limit
               :data (ba/byte-array [66 67])
               :other-data (ba/byte-array [123 123])}
              {:qty-requested 4
               :qty-added 4
               :current-qty 4
               :req {:sku 10 :qty-requested 4}
               :the-reason-why :all
               :data (ba/byte-array [100 110])
               :other-data (ba/byte-array [64 74])}]
        encoded (l/serialize rsps-schema data)
        decoded (deserialize-same rsps-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [4 -10 1 8 20 -10 1 -10 1 4 66 67 4 123 123
                         8 8 8 20 8 0 100 110 4 64 74 0])
         encoded))
    (is (= (xf-byte-arrays data)
           (xf-byte-arrays decoded)))))

(deftest test-nested-map-schema
  (is (= {:type :map
          :values
          {:namespace "deercreeklabs.lancaster-test"
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
             :default "MX"}
            {:name :other-data :type :bytes :default ""}]}}
         (l/get-edn-schema nested-map-schema)))
  (is (= "WjAyzZpCvsP3vrmPavaK1w=="
         (ba/byte-array->b64 (l/get-fingerprint128 nested-map-schema)))))

(deftest test-nested-map-schema-serdes
  (let [data {"A" {:qty-requested 123
                   :qty-added 4
                   :current-qty 10
                   :req {:sku 123 :qty-requested 123}
                   :the-reason-why :limit
                   :data (ba/byte-array [66 67])
                   :other-data (ba/byte-array [123 123])}
              "B" {:qty-requested 4
                   :qty-added 4
                   :current-qty 4
                   :req {:sku 10 :qty-requested 4}
                   :the-reason-why :all
                   :data (ba/byte-array [100 110])
                   :other-data (ba/byte-array [64 74])}}
        encoded (l/serialize nested-map-schema data)
        decoded (deserialize-same nested-map-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [4 2 65 -10 1 8 20 -10 1 -10 1 4 66 67 4
                         123 123 2 66 8 8 8 20 8 0 100 110 4 64 74 0])
         encoded))
    (is (= (xf-byte-arrays data)
           (xf-byte-arrays decoded)))))

(deftest test-union-schema
  (is (= [:int
          {:namespace "deercreeklabs.lancaster-test"
           :name :add-to-cart-req
           :type :record
           :fields
           [{:name :sku :type :int :default -1}
            {:name :qty-requested :type :int :default 0}]}
          {:namespace "deercreeklabs.lancaster-test"
           :name :a-fixed
           :type :fixed
           :size 2}]
         (l/get-edn-schema union-schema)))
  (is (= "eoZHecFG3sfECWnaKrqwtQ=="
         (ba/byte-array->b64 (l/get-fingerprint128 union-schema)))))

(deftest test-union-schema-serdes
  (let [data {:sku 123 :qty-requested 4}
        encoded (l/serialize union-schema data)
        decoded (deserialize-same union-schema encoded)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [2 -10 1 8])
                                          encoded))
        _ (is (= data decoded))
        data 5
        encoded (l/serialize union-schema data)
        decoded (deserialize-same union-schema encoded)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [0 10])
                                          encoded))
        _ (is (= data decoded))]))

(deftest test-recursive-schema
  (is (= {:namespace "deercreeklabs.lancaster-test"
          :name :tree
          :type :record
          :fields
          [{:name :value :type :int :default -1}
           {:name :right
            :type [:null "deercreeklabs.lancaster-test.tree"]
            :default nil}
           {:name :left
            :type [:null "deercreeklabs.lancaster-test.tree"]
            :default nil}]}
         (l/get-edn-schema tree-schema)))
  (is (= "7S/pDrRPpfq+cxuNpMNKlw=="
         (ba/byte-array->b64 (l/get-fingerprint128 tree-schema)))))

(deftest test-recursive-schema-serdes
  (let [data {:value 5
              :right {:value -10
                      :right {:value -20
                             :right nil
                             :left nil}
                      :left nil}
              :left {:value 10
                     :right nil
                     :left {:value 20
                            :right nil
                            :left {:value 40
                                   :right nil
                                   :left nil}}}}
        encoded (l/serialize tree-schema data)
        decoded (deserialize-same tree-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [10 2 19 2 39 0 0 0 2 20 0 2 40 0 2 80 0 0])
               encoded))
        _ (is (= data decoded))]))

(deftest test-schema-evolution-add-a-field
  (let [data {:sku 789
              :qty-requested 10}
        encoded-orig (l/serialize add-to-cart-req-schema data)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [-86 12 20])
                                          encoded-orig))
        decoded-new (l/deserialize add-to-cart-req-v2-schema
                                   (l/get-json-schema add-to-cart-req-schema)
                                   encoded-orig)]
    (is (= (assoc data :username "") decoded-new))))

(deftest test-schema-evolution-remove-a-field
  (let [data {:username ""
              :sku 789
              :qty-requested 10}
        encoded-orig (l/serialize add-to-cart-req-v2-schema data)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [0 -86 12 20])
                                          encoded-orig))
        decoded-new (l/deserialize add-to-cart-req-schema
                                   (l/get-json-schema add-to-cart-req-v2-schema)
                                   encoded-orig)]
    (is (= (dissoc data :username) decoded-new))))
