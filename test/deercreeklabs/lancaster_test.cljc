(ns deercreeklabs.lancaster-test
  (:require
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

;; Make cljs byte-arrays ISeqable
#?(:cljs (extend-protocol ISeqable
           js/Int8Array
           (-seq [o]
             (array-seq o))))

(defn deserialize-same
  "Deserialize with the same reader and writer schemas. Use for testing only."
  [schema encoded]
  (l/deserialize schema (l/get-parsing-canonical-form schema) encoded))

(def add-to-cart-req-schema
  (l/make-record-schema ::add-to-cart-req
                        [[:sku l/int-schema]
                         [:qty-requested l/int-schema 0]]))

(def why-schema (l/make-enum-schema ::why
                                    [:all :stock :limit]))

(def a-fixed-schema (l/make-fixed-schema ::a-fixed 2))

(def rec-w-fixed-no-default-schema
  (l/make-record-schema ::rec-w-fixed-no-default
                        [[:data a-fixed-schema]]))

(def add-to-cart-rsp-schema
  (l/make-record-schema ::add-to-cart-rsp
                        [[:qty-requested l/int-schema]
                         [:qty-added l/int-schema]
                         [:current-qty l/int-schema]
                         [:req add-to-cart-req-schema
                          {:sku 10 :qty-requested 1}]
                         [:the-reason-why why-schema :stock]
                         [:data a-fixed-schema (ba/byte-array [77 88])]
                         [:other-data l/bytes-schema]]))

(def simple-array-schema (l/make-array-schema l/string-schema))

(def rsps-schema (l/make-array-schema add-to-cart-rsp-schema))

(def rec-w-array-and-enum-schema
  (l/make-record-schema ::rec-w-array-and-enum
                        [[:names simple-array-schema]
                         [:why why-schema]]))

(def ages-schema (l/make-map-schema l/int-schema))

(def rec-w-map-schema
  (l/make-record-schema ::rec-w-map
                        [[:name-to-age ages-schema]
                         [:what l/string-schema]]))

(def nested-map-schema (l/make-map-schema add-to-cart-rsp-schema))

(def union-schema
  (l/make-union-schema [l/int-schema add-to-cart-req-schema a-fixed-schema]))

(def person-schema
  (l/make-record-schema ::person
                        [[:name l/string-schema "No name"]
                         [:age l/int-schema 0]]))

(def dog-schema
  (l/make-record-schema ::dog
                        [[:name l/string-schema "No name"]
                         [:owner l/string-schema "No owner"]]))

(def person-or-dog-schema
  (l/make-union-schema [person-schema dog-schema]))

(def map-or-array-schema
  (l/make-union-schema [ages-schema simple-array-schema]))

(def mopodoa-schema
  (l/make-union-schema [ages-schema person-schema dog-schema
                        simple-array-schema]))

;; TODO: Enable recursive schemas
#_
(def tree-schema
  (l/make-record-schema ::tree
                        [[:value l/int-schema]
                         [:right :nil-or-recur]
                         [:left :nil-or-recur]]))

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
    (is (= "45c60aabcfd650ea"
           (u/long->hex-str (l/get-fingerprint64 add-to-cart-req-schema))))
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
  (is (= "6222a8eaeb7a985e"
         (u/long->hex-str (l/get-fingerprint64 why-schema)))))

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
  (is (= "6ded13577f54ade7"
         (u/long->hex-str (l/get-fingerprint64 a-fixed-schema)))))

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
  (is (= "b2872b2c6000c8e5"
         (u/long->hex-str (l/get-fingerprint64 add-to-cart-rsp-schema)))))

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

(defn get-abs-err [expected actual]
  (let [err (- expected actual)]
    (if (neg? err)
      (- err)
      err)))

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
  (is (= "db39e2c2534c8973"
         (u/long->hex-str (l/get-fingerprint64 ages-schema)))))

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
  (is (= "ce5b3256262fd79f"
         (u/long->hex-str (l/get-fingerprint64 simple-array-schema)))))

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
  (is (= "53e478ee274a9db7"
         (u/long->hex-str (l/get-fingerprint64 rsps-schema)))))

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
  (is (=  "9fa5480152c68cf9"
          (u/long->hex-str (l/get-fingerprint64 nested-map-schema)))))

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
  (is (= "ef20e3c27ecf9914"
         (u/long->hex-str (l/get-fingerprint64 union-schema)))))

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
  (is (= "723566f27583c594"
         (u/long->hex-str (l/get-fingerprint64 person-or-dog-schema)))))

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

(deftest test-map-or-array-schema
  (is (= [{:type :map :values :int}
          {:type :array :items :string}]
         (l/get-edn-schema map-or-array-schema)))
  (is (= "3da32ade40155b97"
         (u/long->hex-str (l/get-fingerprint64 map-or-array-schema)))))

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
  (is (= "e206da39366e5f63"
         (u/long->hex-str (l/get-fingerprint64 mopodoa-schema)))))

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
;;          (u/long->hex-str (l/get-fingerprint64 tree-schema)))))

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

;; (deftest test-schema-evolution-add-a-field
;;   (let [data {:sku 789
;;               :qty-requested 10}
;;         encoded-orig (l/serialize add-to-cart-req-schema data)
;;         _ (is (ba/equivalent-byte-arrays? (ba/byte-array [-86 12 20])
;;                                           encoded-orig))
;;         decoded-new (l/deserialize add-to-cart-req-v2-schema
;;                                    (l/get-parsing-canonical-form
;;                                     add-to-cart-req-schema)
;;                                    encoded-orig)]
;;     (is (= (assoc data :username "") decoded-new))))

;; (deftest test-schema-evolution-remove-a-field
;;   (let [data {:username ""
;;               :sku 789
;;               :qty-requested 10}
;;         encoded-orig (l/serialize add-to-cart-req-v2-schema data)
;;         _ (is (ba/equivalent-byte-arrays? (ba/byte-array [0 -86 12 20])
;;                                           encoded-orig))
;;         decoded-new (l/deserialize
;;                      add-to-cart-req-v3-schema
;;                      (l/get-parsing-canonical-form add-to-cart-req-v2-schema)
;;                      encoded-orig)]
;;     (is (= (dissoc data :username) decoded-new))))

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
  (is (= "91fa1c9b7aa30f5a"
         (u/long->hex-str (l/get-fingerprint64
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
  (is (= "a83fbba8cc587ad3"
         (u/long->hex-str (l/get-fingerprint64
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
  (is (= "c257b331f3becb0c"
         (u/long->hex-str (l/get-fingerprint64
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
