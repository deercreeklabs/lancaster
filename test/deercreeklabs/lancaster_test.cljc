(ns deercreeklabs.lancaster-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.walk :as walk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! false)

(u/configure-logging)

(defn deserialize-same
  "Deserialize with the same reader and writer schemas. Use for testing only."
  [schema encoded]
  (l/deserialize schema (l/get-parsing-canonical-form schema) encoded))

(defn get-abs-err [expected actual]
  (let [err (- expected actual)]
    (if (neg? err)
      (- err)
      err)))

(defn get-rel-err [expected actual]
  (/ (get-abs-err expected actual) expected))

(l/def-record-schema add-to-cart-req-schema
  [:sku l/int-schema]
  [:qty-requested l/int-schema 0])

(def add-to-cart-req-v2-schema
  (l/make-record-schema ::add-to-cart-req
                        [[:sku l/int-schema]
                         [:qty-requested l/int-schema 0]
                         [:note l/string-schema "No note"]]))

(def add-to-cart-req-v3-schema ;; qtys are floats!
  (l/make-record-schema ::add-to-cart-req
                        [[:sku l/int-schema]
                         [:qty-requested l/float-schema]
                         [:note l/string-schema "No note"]]))

(l/def-enum-schema why-schema
  :all :stock :limit)

(l/def-fixed-schema a-fixed-schema
  2)

(l/def-record-schema rec-w-fixed-no-default-schema
  [:data a-fixed-schema])

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested l/int-schema]
  [:qty-added l/int-schema]
  [:current-qty l/int-schema]
  [:req add-to-cart-req-schema
   {:sku 10 :qty-requested 1}]
  [:the-reason-why why-schema :stock]
  [:data a-fixed-schema (ba/byte-array [77 88])]
  [:other-data l/bytes-schema])

(def simple-array-schema (l/make-array-schema l/string-schema))

(def rsps-schema (l/make-array-schema add-to-cart-rsp-schema))

(l/def-record-schema rec-w-array-and-enum-schema
  [:names simple-array-schema]
  [:why why-schema])

(def ages-schema (l/make-map-schema l/int-schema))

(l/def-record-schema rec-w-map-schema
  [:name-to-age ages-schema]
  [:what l/string-schema])

(def nested-map-schema (l/make-map-schema add-to-cart-rsp-schema))

(def union-schema
  (l/make-union-schema [l/int-schema add-to-cart-req-schema a-fixed-schema]))

(l/def-record-schema person-schema
  [:name l/string-schema "No name"]
  [:age l/int-schema 0])

(l/def-record-schema dog-schema
  [:name l/string-schema "No name"]
  [:owner l/string-schema "No owner"])

(def dog-v2-schema
  (l/make-record-schema ::dog
                        [[:name l/string-schema "No name"]
                         [:owner l/string-schema "No owner"]
                         [:tag-number l/int-schema]]))

(l/def-record-schema fish-schema
  [:name l/string-schema "No name"]
  [:tank-num l/int-schema])

(def person-or-dog-schema
  (l/make-union-schema [person-schema dog-schema]))

(def fish-or-person-or-dog-v2-schema
  (l/make-union-schema [fish-schema person-schema dog-v2-schema]))

(def map-or-array-schema
  (l/make-union-schema [ages-schema simple-array-schema]))

(def mopodoa-schema
  (l/make-union-schema [ages-schema person-schema dog-schema
                        simple-array-schema]))

;; TODO: Enable recursive schemas
#_
(l/def-record-schema tree-schema
  [:value l/int-schema]
  [:right :nil-or-recur]
  [:left :nil-or-recur])

(deftest test-record-schema
  (let [expected-pcf (str "{\"name\":\"deercreeklabs.lancaster_test."
                          "AddToCartReq\",\"type\":\"record\",\"fields\":"
                          "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                          "\"qtyRequested\",\"type\":\"int\"}]}")
        expected-edn-schema {:namespace :deercreeklabs.lancaster-test
                             :name :add-to-cart-req
                             :type :record
                             :fields
                             [{:name :sku
                               :type :int
                               :default -1}
                              {:name :qty-requested
                               :type :int
                               :default 0}]}]
    (is (= "5027717767048351978"
           (u/long->str (l/get-fingerprint64 add-to-cart-req-schema))))
    (is (= expected-edn-schema (l/get-edn-schema add-to-cart-req-schema)))
    (is (= expected-pcf (l/get-parsing-canonical-form
                         add-to-cart-req-schema)))))

(deftest test-def-record-schema-serdes
  (let [data {:sku 123
              :qty-requested 5}
        encoded (l/serialize add-to-cart-req-schema data)
        decoded (deserialize-same add-to-cart-req-schema encoded)]
    (is (= "9gEK" (ba/byte-array->b64 encoded)))
    (is (= data decoded))))

(deftest test-def-enum-schema
  (is (= {:namespace :deercreeklabs.lancaster-test
          :name :why
          :type :enum
          :symbols [:all :stock :limit]}
         (l/get-edn-schema why-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.Why\",\"type\":"
              "\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}")
         (l/get-parsing-canonical-form why-schema)))
  (is (= "7071400091851593822"
         (u/long->str (l/get-fingerprint64 why-schema)))))

(deftest test-def-enum-schema-serdes
  (let [data :stock
        encoded (l/serialize why-schema data)
        decoded (deserialize-same why-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [2]) encoded))
    (is (= data decoded))))

(deftest test-def-fixed-schema
  (is (= {:namespace :deercreeklabs.lancaster-test
          :name :a-fixed
          :type :fixed
          :size 2}
         (l/get-edn-schema a-fixed-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.AFixed\",\"type\":"
              "\"fixed\",\"size\":2}")
         (l/get-parsing-canonical-form a-fixed-schema)))
  (is (= "7921008586133908967"
         (u/long->str (l/get-fingerprint64 a-fixed-schema)))))

(deftest test-def-fixed-schema-serdes
  (let [data (ba/byte-array [12 24])
        encoded (l/serialize a-fixed-schema data)
        decoded (deserialize-same a-fixed-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         data encoded))
    (is (ba/equivalent-byte-arrays? data decoded))))

(defn xf-byte-arrays
  [edn-schema]
  (walk/postwalk #(if (ba/byte-array? %)
                    (u/byte-array->byte-str %)
                    %)
                 edn-schema))

(deftest test-nested-record-schema
  (let [expected {:namespace :deercreeklabs.lancaster-test
                  :name :add-to-cart-rsp
                  :type :record
                  :fields
                  [{:name :qty-requested :type :int :default -1}
                   {:name :qty-added :type :int :default -1}
                   {:name :current-qty :type :int :default -1}
                   {:name :req
                    :type
                    {:namespace :deercreeklabs.lancaster-test
                     :name :add-to-cart-req
                     :type :record
                     :fields
                     [{:name :sku :type :int :default -1}
                      {:name :qty-requested :type :int :default 0}]}
                    :default {:sku 10 :qty-requested 1}}
                   {:name :the-reason-why
                    :type
                    {:namespace :deercreeklabs.lancaster-test
                     :name :why
                     :type :enum
                     :symbols [:all :stock :limit]}
                    :default :stock}
                   {:name :data
                    :type
                    {:namespace :deercreeklabs.lancaster-test
                     :name :a-fixed
                     :type :fixed
                     :size 2}
                    :default "MX"}
                   {:name :other-data
                    :type :bytes
                    :default ""}]}]
    (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.AddToCartRsp\","
                "\"type\":\"record\",\"fields\":[{\"name\":\"qtyRequested\","
                "\"type\":\"int\"},{\"name\":\"qtyAdded\",\"type\":\"int\"},"
                "{\"name\":\"currentQty\",\"type\":\"int\"},{\"name\":\"req\","
                "\"type\":{\"name\":\"deercreeklabs.lancaster_test."
                "AddToCartReq\",\"type\":\"record\",\"fields\":[{\"name\":"
                "\"sku\",\"type\":\"int\"},{\"name\":\"qtyRequested\",\"type"
                "\":\"int\"}]}},{\"name\":\"theReasonWhy\",\"type\":{\"name\":"
                "\"deercreeklabs.lancaster_test.Why\",\"type\":\"enum\","
                "\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}},{\"name\":"
                "\"data\",\"type\":{\"name\":\"deercreeklabs.lancaster_test."
                "AFixed\",\"type\":\"fixed\",\"size\":2}},{\"name\":"
                "\"otherData\",\"type\":\"bytes\"}]}")
           (l/get-parsing-canonical-form add-to-cart-rsp-schema)))
    (is (= expected (l/get-edn-schema add-to-cart-rsp-schema))))
  (is (= "-5582445743513220891"
         (u/long->str (l/get-fingerprint64 add-to-cart-rsp-schema)))))

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

(deftest test-int-schema-serdes
  (let [data 7890
        encoded (l/serialize l/int-schema data)
        decoded (deserialize-same l/int-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-92 123]) encoded))
    (is (= data decoded))))

(deftest test-long-schema-serdes
  (let [data (u/ints->long 2147483647 -1)
        encoded (l/serialize l/long-schema data)
        decoded (deserialize-same l/long-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [-2 -1 -1 -1 -1 -1 -1 -1 -1 1])
         encoded))
    (is (= data decoded))))

(deftest test-int->long
  (let [in (int -1)
        l (u/int->long in)]
    (is (= "-1" (u/long->str l)))))

(deftest test-float-schema
  (let [data (float 3.14159)
        encoded (l/serialize l/float-schema data)
        decoded (deserialize-same l/float-schema encoded)
        abs-err (get-abs-err data decoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-48 15 73 64]) encoded))
    (is (< abs-err 0.000001))))

(deftest test-double-schema
  (let [data (double 3.14159265359)
        encoded (l/serialize l/double-schema data)
        decoded (deserialize-same l/double-schema encoded)
        abs-err (get-abs-err data decoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-22 46 68 84 -5 33 9 64])
                                    encoded))
    (is (< abs-err 0.000001))))

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
  (is (= "-2649837581481768589"
         (u/long->str (l/get-fingerprint64 ages-schema)))))

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
    (is (= data decoded))))

(deftest test-def-array-schema
  (is (= {:type :array :items :string}
         (l/get-edn-schema simple-array-schema)))
  (is (= "{\"type\":\"array\",\"items\":\"string\"}"
         (l/get-parsing-canonical-form simple-array-schema)))
  (is (= "-3577210133426481249"
         (u/long->str (l/get-fingerprint64 simple-array-schema)))))

(deftest test-array-schema-serdes
  (let [names ["Ferdinand" "Omar" "Lin"]
        encoded (l/serialize simple-array-schema names)
        decoded (deserialize-same simple-array-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 18 70 101 114 100 105 110 97 110 100 8 79
                         109 97 114 6 76 105 110 0])
         encoded))
    (is (= names decoded))))

(deftest test-nested-array-schema
  (is (= {:type :array
          :items
          {:namespace :deercreeklabs.lancaster-test
           :name :add-to-cart-rsp
           :type :record
           :fields
           [{:name :qty-requested :type :int :default -1}
            {:name :qty-added :type :int :default -1}
            {:name :current-qty :type :int :default -1}
            {:name :req
             :type
             {:namespace :deercreeklabs.lancaster-test
              :name :add-to-cart-req
              :type :record
              :fields
              [{:name :sku :type :int :default -1}
               {:name :qty-requested :type :int :default 0}]}
             :default {:sku 10 :qty-requested 1}}
            {:name :the-reason-why
             :type
             {:namespace :deercreeklabs.lancaster-test
              :name :why
              :type :enum
              :symbols [:all :stock :limit]}
             :default :stock}
            {:name :data
             :type
             {:namespace :deercreeklabs.lancaster-test
              :name :a-fixed
              :type :fixed
              :size 2}
             :default "MX"}
            {:name :other-data :type :bytes :default ""}]}}
         (l/get-edn-schema rsps-schema)))
  (is (= "6045089564094799287"
         (u/long->str (l/get-fingerprint64 rsps-schema)))))

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
          {:namespace :deercreeklabs.lancaster-test
           :name :add-to-cart-rsp
           :type :record
           :fields
           [{:name :qty-requested :type :int :default -1}
            {:name :qty-added :type :int :default -1}
            {:name :current-qty :type :int :default -1}
            {:name :req
             :type
             {:namespace :deercreeklabs.lancaster-test
              :name :add-to-cart-req
              :type :record
              :fields
              [{:name :sku :type :int :default -1}
               {:name :qty-requested :type :int :default 0}]}
             :default {:sku 10 :qty-requested 1}}
            {:name :the-reason-why
             :type
             {:namespace :deercreeklabs.lancaster-test
              :name :why
              :type :enum
              :symbols [:all :stock :limit]}
             :default :stock}
            {:name :data
             :type
             {:namespace :deercreeklabs.lancaster-test
              :name :a-fixed
              :type :fixed
              :size 2}
             :default "MX"}
            {:name :other-data :type :bytes :default ""}]}}
         (l/get-edn-schema nested-map-schema)))
  (is (= "-6943064080000840455"
          (u/long->str (l/get-fingerprint64 nested-map-schema)))))

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
          {:namespace :deercreeklabs.lancaster-test
           :name :add-to-cart-req
           :type :record
           :fields
           [{:name :sku :type :int :default -1}
            {:name :qty-requested :type :int :default 0}]}
          {:namespace :deercreeklabs.lancaster-test
           :name :a-fixed
           :type :fixed
           :size 2}]
         (l/get-edn-schema union-schema)))
  (is (= "-1215721474899338988"
         (u/long->str (l/get-fingerprint64 union-schema)))))

(deftest test-union-schema-serdes
  (let [data {:sku 123 :qty-requested 4}
        encoded (l/serialize union-schema data)
        decoded (deserialize-same union-schema encoded)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [2 -10 1 8])
                                          encoded))
        _ (is (= data decoded))
        data 5
        encoded (l/serialize union-schema data)
        decoded (deserialize-same union-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [0 10])
                                    encoded))
    (is (= data decoded))))

(deftest test-wrapped-union-schema
  (is (= [{:namespace :deercreeklabs.lancaster-test
           :name :person
           :type :record
           :fields
           [{:name :name :type :string :default "No name"}
            {:name :age :type :int :default 0}]}
          {:namespace :deercreeklabs.lancaster-test
           :name :dog
           :type :record
           :fields
           [{:name :name :type :string :default "No name"}
            {:name :owner :type :string :default "No owner"}]}]
         (l/get-edn-schema person-or-dog-schema)))
  (is (= "8229597085629138324"
         (u/long->str (l/get-fingerprint64 person-or-dog-schema)))))

(deftest test-wrapped-union-schema-serdes
  (let [data (l/wrap dog-schema {:name "Fido" :owner "Zach"})
        encoded (l/serialize person-or-dog-schema data)
        decoded (deserialize-same person-or-dog-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [2 8 70 105 100 111 8 90 97 99 104])
               encoded))
        _ (is (= data decoded))
        data (l/wrap person-schema {:name "Bill" :age 50})
        encoded (l/serialize person-or-dog-schema data)
        decoded (deserialize-same person-or-dog-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [0 8 66 105 108 108 100])
         encoded))
    (is (= data decoded))))

(deftest test-wrapped-union-schema-serdes-non-existent
  (try
    (l/serialize person-or-dog-schema [:non-existent-schema-name {}])
    (is (= :did-not-throw :but-should-have))
    (catch #?(:clj Exception :cljs js/Error) e
      (let [msg (lu/get-exception-msg e)]
        (is (str/includes?
             msg "`:non-existent-schema-name` is not in the union schema"))))))

(deftest test-map-or-array-schema
  (is (= [{:type :map :values :int}
          {:type :array :items :string}]
         (l/get-edn-schema map-or-array-schema)))
  (is (= "4441440791563688855"
         (u/long->str (l/get-fingerprint64 map-or-array-schema)))))

(deftest test-map-or-array-schema-serdes
  (let [data {"Zeke" 22 "Adeline" 88}
        encoded (l/serialize map-or-array-schema data)
        decoded (deserialize-same map-or-array-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0 4 8 90 101 107 101 44 14 65 100 101 108 105
                               110 101 -80 1 0])
               encoded))
        _ (is (= data decoded))
        data ["a thing" "another thing"]
        encoded (l/serialize map-or-array-schema data)
        decoded (deserialize-same map-or-array-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [2 4 14 97 32 116 104 105 110 103 26 97 110
                         111 116 104 101 114 32 116 104 105 110 103 0])
         encoded))
    (is (= data decoded))))

(deftest test-mopodoa-schema
  (is (= [{:type :map :values :int}
          {:namespace :deercreeklabs.lancaster-test
           :name :person
           :type :record
           :fields [{:name :name :type :string :default "No name"}
                    {:name :age :type :int :default 0}]}
          {:namespace :deercreeklabs.lancaster-test
           :name :dog
           :type :record
           :fields [{:name :name :type :string :default "No name"}
                    {:name :owner :type :string :default "No owner"}]}
          {:type :array :items :string}]
         (l/get-edn-schema mopodoa-schema)))
  (is (= "-2159799032016380061"
         (u/long->str (l/get-fingerprint64 mopodoa-schema)))))

(deftest test-mopodoa-schema-serdes
  (let [data (l/wrap ages-schema {"Zeke" 22 "Adeline" 88})
        encoded (l/serialize mopodoa-schema data)
        decoded (deserialize-same mopodoa-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0 4 8 90 101 107 101 44 14 65 100 101 108 105
                               110 101 -80 1 0])
               encoded))
        _ (is (= data decoded))
        data (l/wrap simple-array-schema ["a thing" "another thing"])
        encoded (l/serialize mopodoa-schema data)
        decoded (deserialize-same mopodoa-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 4 14 97 32 116 104 105 110 103 26 97 110
                         111 116 104 101 114 32 116 104 105 110 103 0])
         encoded))
    (is (= data decoded))))

;; (deftest test-recursive-schema
;;   (is (= {:namespace "deercreeklabs.lancaster-test"
;;           :name :tree
;;           :type :record
;;           :fields
;;           [{:name :value :type :int :default -1}
;;            {:name :right
;;             :type [:null "deercreeklabs.lancaster-test.tree"]
;;             :default nil}
;;            {:name :left
;;             :type [:null "deercreeklabs.lancaster-test.tree"]
;;             :default nil}]}
;;          (l/get-edn-schema tree-schema)))
;;   (is (= "7S/pDrRPpfq+cxuNpMNKlw=="
;;          (u/long->str (l/get-fingerprint64 tree-schema)))))

;; (deftest test-recursive-schema-serdes
;;   (let [data {:value 5
;;               :right {:value -10
;;                       :right {:value -20
;;                               :right nil
;;                               :left nil}
;;                       :left nil}
;;               :left {:value 10
;;                      :right nil
;;                      :left {:value 20
;;                             :right nil
;;                             :left {:value 40
;;                                    :right nil
;;                                    :left nil}}}}
;;         encoded (l/serialize tree-schema data)
;;         decoded (deserialize-same tree-schema encoded)
;;         _ (is (ba/equivalent-byte-arrays?
;;                (ba/byte-array [10 2 19 2 39 0 0 0 2 20 0 2 40 0 2 80 0 0])
;;                encoded))
;;         _ (is (= data decoded))]))

(deftest test-schema-resolution-int-to-long
  (let [data 10
        writer-schema l/int-schema
        reader-schema l/long-schema
        encoded-orig (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded-new (l/deserialize reader-schema writer-pcf encoded-orig)]
    (is (= "10" (u/long->str decoded-new)))))

(deftest test-schema-resolution-int-to-float
  (let [data 10
        writer-schema l/int-schema
        reader-schema l/float-schema
        encoded-orig (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded-new (l/deserialize reader-schema writer-pcf encoded-orig)]
    (is (= (float data) decoded-new))))

(deftest test-schema-resolution-int-to-double
  (let [data 10
        writer-schema l/int-schema
        reader-schema l/float-schema
        encoded-orig (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded-new (l/deserialize reader-schema writer-pcf encoded-orig)]
    (is (= (double data) decoded-new))))

(deftest test-schema-resolution-long-to-float
  (let [data (u/ints->long 12345 6789)
        writer-schema l/long-schema
        reader-schema l/float-schema
        encoded-orig (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded-orig)
        expected 5.3021371E13
        rel-err (get-rel-err expected decoded)]
    (is (> 0.00000001 rel-err))))

(deftest test-schema-resolution-long-to-double
  (let [data (u/ints->long -12345 -6789)
        writer-schema l/long-schema
        reader-schema l/double-schema
        encoded-orig (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded-orig)
        expected (double -53017076308613)
        rel-err (get-rel-err expected decoded)]
    (is (> 0.00000001 rel-err))))

(deftest test-schema-resolution-float-to-double
  (let [data (float 1234.5789)
        writer-schema l/float-schema
        reader-schema l/double-schema
        encoded-orig (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded-orig)
        rel-err (get-rel-err data decoded)]
    (is (> 0.0000001 rel-err))))

(deftest test-schema-resolution-string-to-bytes
  (let [data "Hello, World!"
        writer-schema l/string-schema
        reader-schema l/bytes-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        expected (ba/byte-array [72 101 108 108 111 44
                                 32 87 111 114 108 100 33])]
    (is (ba/equivalent-byte-arrays? expected decoded))))

(deftest test-schema-resolution-int-array-to-float-array
  (let [data [1 2 3]
        writer-schema (l/make-array-schema l/int-schema)
        reader-schema (l/make-array-schema l/float-schema)
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        expected [1.0 2.0 3.0]]
    (is (= expected decoded))))

(deftest test-schema-resolution-int-map-to-float-map
  (let [data {"one" 1 "two" 2}
        writer-schema (l/make-map-schema l/int-schema)
        reader-schema (l/make-map-schema l/float-schema)
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        expected {"one" 1.0 "two" 2.0}]
    (is (= expected decoded))))

(deftest test-schema-resolution-enum-added-symbol
  (let [data :stock
        writer-schema why-schema
        reader-schema (l/make-enum-schema ::why
                                          [:foo :all :limit :stock])
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= data decoded))))

(deftest test-record-schema-evolution-add-field
  (let [data {:sku 789
              :qty-requested 10}
        writer-schema add-to-cart-req-schema
        reader-schema add-to-cart-req-v2-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= (assoc data :note "No note") decoded))))

(deftest test-schema-evolution-remove-field
  (let [data {:sku 789
              :qty-requested 10
              :note "This is a nice item"}
        writer-schema add-to-cart-req-v2-schema
        reader-schema add-to-cart-req-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= (dissoc data :note) decoded))))

(deftest test-schema-evolution-change-field
  (let [data {:sku 123
              :qty-requested 10
              :note "This is a nice item"}
        writer-schema add-to-cart-req-v2-schema
        reader-schema add-to-cart-req-v3-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= (assoc data :qty-requested 10.0) decoded))))

(deftest test-schema-evolution-add-field-and-change-field
  (let [data {:sku 123
              :qty-requested 10}
        writer-schema add-to-cart-req-schema
        reader-schema add-to-cart-req-v3-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= (assoc data :qty-requested 10.0 :note "No note") decoded))))

(deftest test-schema-evolution-union-add-member
  (let [data (l/wrap dog-schema {:name "Rover" :owner "Zeus"})
        writer-schema person-or-dog-schema
        reader-schema fish-or-person-or-dog-v2-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        [dec-sch-name dec-data] decoded
        [orig-sch-name orig-data] data]
    (is (= orig-sch-name dec-sch-name))
    (is (= (assoc orig-data :tag-number -1)
           dec-data))))

(deftest test-schema-evolution-union-to-non-union
  (let [data {:name "Rover" :owner "Zeus"}
        wrapped-data (l/wrap dog-schema data)
        writer-schema person-or-dog-schema
        reader-schema dog-v2-schema
        encoded (l/serialize writer-schema wrapped-data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= (assoc data :tag-number -1) decoded))))

(deftest test-schema-evolution-non-union-to-union
  (let [data {:name "Rover" :owner "Zeus" :tag-number 123}
        wrapped-data (l/wrap dog-schema data)
        writer-schema dog-v2-schema
        reader-schema person-or-dog-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        expected (l/wrap dog-schema (dissoc data :tag-number))]
    (is (= expected decoded))))

(deftest test-schema-evolution-union-remove-member-success
  (let [data {:name "Runner" :owner "Tommy"}
        wrapped-data (l/wrap dog-schema data)
        writer-schema fish-or-person-or-dog-v2-schema
        reader-schema person-or-dog-schema
        encoded (l/serialize writer-schema wrapped-data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)]
    (is (= wrapped-data decoded))))

(deftest test-schema-evolution-union-remove-member-failure
  (let [data {:name "Swimmy" :tank-num 24}
        wrapped-data (l/wrap fish-schema data)
        writer-schema fish-or-person-or-dog-v2-schema
        reader-schema person-or-dog-schema
        encoded (l/serialize writer-schema wrapped-data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)]
    (try
      (l/deserialize reader-schema writer-pcf encoded)
      (is (= :did-not-throw :but-should-have))
      (catch #?(:clj Exception :cljs js/Error) e
        (let [msg (lu/get-exception-msg e)]
          (is (str/includes? msg "do not match.")))))))

(deftest test-schema-evolution-no-match
  (let [data {:sku 123
              :qty-requested 10}
        writer-schema add-to-cart-req-schema
        reader-schema l/int-schema
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)]
    (try
      (l/deserialize reader-schema writer-pcf encoded)
      (is (= :did-not-throw :but-should-have))
      (catch #?(:clj Exception :cljs js/Error) e
        (let [msg (lu/get-exception-msg e)]
          (is (str/includes? msg "do not match.")))))))

(deftest test-rec-w-array-and-enum-schema
  (is (= {:namespace :deercreeklabs.lancaster-test
          :name :rec-w-array-and-enum
          :type :record
          :fields
          [{:name :names
            :type {:type :array :items :string}
            :default []}
           {:name :why
            :type
            {:namespace :deercreeklabs.lancaster-test
             :name :why
             :type :enum
             :symbols [:all :stock :limit]}
            :default :all}]}
         (l/get-edn-schema rec-w-array-and-enum-schema)))
  (is (= "-7927992739929321638"
         (u/long->str (l/get-fingerprint64
                           rec-w-array-and-enum-schema)))))

(deftest test-rec-w-array-and-enum-serdes
  (let [data {:names ["Aria" "Beth" "Cindy"]
              :why :stock}
        encoded (l/serialize rec-w-array-and-enum-schema data)
        decoded (deserialize-same rec-w-array-and-enum-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 8 65 114 105 97 8 66 101 116 104 10 67 105
                         110 100 121 0 2])
         encoded))
    (is (= data decoded))))

(deftest test-rec-w-map-schema
  (is (= {:namespace :deercreeklabs.lancaster-test
          :name :rec-w-map
          :type :record
          :fields
          [{:name :name-to-age
            :type {:type :map :values :int}
            :default {}}
           {:name :what :type :string :default ""}]}
         (l/get-edn-schema rec-w-map-schema)))
  (is (= "-6323129018147636525"
         (u/long->str (l/get-fingerprint64
                           rec-w-map-schema)))))

(deftest test-rec-w-map-serdes
  (let [data {:name-to-age {"Aria" 22
                            "Beth" 33
                            "Cindy" 44}
              :what "yo"}
        encoded (l/serialize rec-w-map-schema data)
        decoded (deserialize-same rec-w-map-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 8 65 114 105 97 44 8 66 101 116 104 66 10
                         67 105 110 100 121 88 0 4 121 111])
         encoded))
    (is (= data decoded))))

(deftest test-rec-w-fixed-no-default
  (is (= {:namespace :deercreeklabs.lancaster-test
          :name :rec-w-fixed-no-default
          :type :record
          :fields
          [{:name :data
            :type
            {:namespace :deercreeklabs.lancaster-test
             :name :a-fixed
             :type :fixed
             :size 2}
            :default "\0\0"}]}
         (l/get-edn-schema rec-w-fixed-no-default-schema)))
  (is (= "-4442885480253568244"
         (u/long->str (l/get-fingerprint64
                           rec-w-fixed-no-default-schema)))))

(deftest test-rec-w-fixed-no-default-serdes
  (let [data {:data (ba/byte-array [1 2])}
        encoded (l/serialize rec-w-fixed-no-default-schema data)
        decoded (deserialize-same rec-w-fixed-no-default-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [1 2])
         encoded))
    (is (ba/equivalent-byte-arrays?
         (:data data) (:data decoded)))))
