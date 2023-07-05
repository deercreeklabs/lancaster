(ns deercreeklabs.unit.lancaster-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.walk :as walk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.pcf-utils :as pcf-utils]
   [deercreeklabs.lancaster.utils :as u])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo)
      (org.apache.avro Schema
                       SchemaNormalization
                       Schema$Parser))))

(defn abs-err [expected actual]
  (let [err (- expected actual)]
    (if (neg? err)
      (- err)
      err)))

(defn rel-err [expected actual]
  (/ (abs-err expected actual) expected))

(defn xf-byte-arrays
  [edn]
  (walk/postwalk #(if (ba/byte-array? %)
                    (u/byte-array->byte-str %)
                    %)
                 edn))

#?(:clj
   (defn fp-matches? [schema]
     (let [json-schema (l/json schema)
           parser (Schema$Parser.)
           java-schema (.parse parser ^String json-schema)
           java-fp (SchemaNormalization/parsingFingerprint64 java-schema)
           clj-fp (l/fingerprint64 schema)]
       (or (= java-fp clj-fp)
           (let [java-pcf (SchemaNormalization/toParsingForm java-schema)
                 clj-pcf (l/pcf schema)
                 err-str (str "Fingerprints do not match!\n"
                              "java-fp: " java-fp "\n"
                              "clj-fp: " clj-fp "\n"
                              "java-pcf:\n" java-pcf "\n"
                              "clj-pcf:\n" clj-pcf "\n")]
             (println err-str))))))

(defn round-trip? [schema data]
  (let [serialized (l/serialize schema data)
        deserialized (l/deserialize-same schema serialized)]
    (= data deserialized)))

(l/def-record-schema add-to-cart-req-schema
  [:sku :required l/int-schema]
  [:qty-requested l/int-schema])

(def add-to-cart-req-v2-schema
  (l/record-schema ::add-to-cart-req
                   [[:sku l/int-schema]
                    [:qty-requested l/int-schema]
                    [:note l/string-schema]]))

(def add-to-cart-req-v3-schema ;; qtys are floats!
  (l/record-schema ::add-to-cart-req
                   [[:sku l/int-schema]
                    [:qty-requested l/float-schema]
                    [:note l/string-schema]]))

(def add-to-cart-req-v4-schema
  (l/record-schema ::add-to-cart-req
                   [[:sku l/int-schema]
                    [:qty-requested l/int-schema]
                    [:comment l/string-schema]]))

(l/def-enum-schema why-schema
  :all :stock :limit)

(l/def-enum-schema suit-schema
  :hearts :clubs :spades :diamonds)

(l/def-fixed-schema a-fixed-schema
  2)

(l/def-maybe-schema maybe-int-schema
  l/int-schema)

(l/def-record-schema rec-w-maybe-field-schema
  [:name l/string-schema]
  [:age maybe-int-schema])

(l/def-record-schema rec-w-fixed-no-default-schema
  [:data :required a-fixed-schema])

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested l/int-schema]
  [:qty-added l/int-schema]
  [:current-qty l/int-schema]
  [:req :required add-to-cart-req-schema {:sku 10 :qty-requested 1}]
  [:the-reason-why :required why-schema :stock]
  [:data :required a-fixed-schema (ba/byte-array [77 88])]
  [:other-data l/bytes-schema])

(l/def-array-schema simple-array-schema
  l/string-schema)

(l/def-array-schema rsps-schema
  add-to-cart-rsp-schema)

(l/def-record-schema rec-w-array-and-enum-schema
  [:names :required simple-array-schema]
  [:why why-schema])

(l/def-map-schema ages-schema
  l/int-schema)

(l/def-record-schema rec-w-map-schema
  [:name-to-age ages-schema]
  [:what l/string-schema])

(l/def-map-schema nested-map-schema
  add-to-cart-rsp-schema)

(l/def-union-schema union-schema
  l/int-schema add-to-cart-req-schema a-fixed-schema)

(l/def-record-schema person-schema
  [:person-name :required l/string-schema "No name"]
  [:age :required l/int-schema 0])

(l/def-record-schema dog-schema
  [:name l/string-schema]
  [:owner l/string-schema])

(def dog-v2-schema
  (l/record-schema ::dog
                   [[:name l/string-schema]
                    [:owner l/string-schema]
                    [:tag-number l/int-schema]]))

(l/def-record-schema fish-schema
  [:fish-name l/string-schema]
  [:tank-num l/int-schema])

(l/def-union-schema person-or-dog-schema
  person-schema dog-schema)

(l/def-union-schema fish-or-person-or-dog-v2-schema
  fish-schema person-schema dog-v2-schema)

(l/def-union-schema map-or-array-schema
  ages-schema simple-array-schema)

(l/def-union-schema mopodoa-schema
  ages-schema person-schema dog-schema simple-array-schema)

(l/def-record-schema date-schema
  [:year l/int-schema]
  [:month l/int-schema]
  [:day l/int-schema])

(l/def-record-schema time-schema
  [:hour l/int-schema]
  [:minute l/int-schema]
  [:second (l/maybe l/int-schema)])

(l/def-record-schema date-time-schema
  ;; Note that this does not include seconds.
  [:year l/int-schema]
  [:month l/int-schema]
  [:day l/int-schema]
  [:hour l/int-schema]
  [:minute l/int-schema])

(l/def-record-schema tree-schema
  [:value :required l/int-schema]
  [:right :tree]
  [:left :tree])

(deftest test-record-schema
  (let [expected-pcf (str
                      "{\"name\":\"deercreeklabs.unit.lancaster_test."
                      "AddToCartReq\",\"type\":\"record\",\"fields\":"
                      "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                      "\"qtyRequested\",\"type\":[\"null\",\"int\"]}]}")
        expected-edn {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                      :type :record
                      :fields
                      [{:name :sku
                        :type :int
                        :default -1}
                       {:name :qty-requested
                        :type [:null :int]
                        :default nil}]}]
    #?(:clj (is (fp-matches? add-to-cart-req-schema)))
    (is (= "3703990475150151424"
           (u/long->str (l/fingerprint64 add-to-cart-req-schema))))
    (is (= expected-edn (l/edn add-to-cart-req-schema)))
    (is (= expected-pcf (l/pcf
                         add-to-cart-req-schema)))))

(deftest test-def-record-schema-serdes
  (let [data {:sku 123
              :qty-requested 5}
        encoded (l/serialize add-to-cart-req-schema data)
        decoded (l/deserialize-same add-to-cart-req-schema encoded)]
    (is (= "9gECCg==" (ba/byte-array->b64 encoded)))
    (is (= data decoded))))

(deftest test-def-record-schema-mixed-ns
  (is (= {:name :deercreeklabs.unit.lancaster-test/fish
          :type :record
          :fields [{:name :fish-name
                    :type [:null :string]
                    :default nil}
                   {:name :tank-num
                    :type [:null :int]
                    :default nil}]}
         (l/edn fish-schema)))
  (is (= (str
          "{\"name\":\"deercreeklabs.unit.lancaster_test.Fish\",\"type\":"
          "\"record\",\"fields\":[{\"name\":\"fishName\",\"type\":[\"null\","
          "\"string\"],\"default\":null},{\"name\":\"tankNum\",\"type\":"
          "[\"null\",\"int\"],\"default\":null}]}")
         (l/json fish-schema))))

(deftest test-def-enum-schema
  (is (= {:name :deercreeklabs.unit.lancaster-test/why
          :type :enum
          :symbols [:all :stock :limit]
          :default :all}
         (l/edn why-schema)))
  #?(:clj (is (fp-matches? why-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.Why\",\"type\":"
              "\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"],\"default\":"
              "\"ALL\"}")
         (l/json why-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.Why\",\"type\":"
              "\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}")
         (l/pcf why-schema)))
  (is (= "3321246333858425949"
         (u/long->str (l/fingerprint64 why-schema)))))

(deftest test-def-enum-schema-serdes
  (let [data :stock
        encoded (l/serialize why-schema data)
        decoded (l/deserialize-same why-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [2]) encoded))
    (is (= data decoded))))

(deftest test-def-fixed-schema
  (is (= {:name :deercreeklabs.unit.lancaster-test/a-fixed
          :type :fixed
          :size 2}
         (l/edn a-fixed-schema)))
  #?(:clj (is (fp-matches? a-fixed-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.AFixed\",\"type\":"
              "\"fixed\",\"size\":2}")
         (l/pcf a-fixed-schema)))
  (is (= "-1031156250377191762"
         (u/long->str (l/fingerprint64 a-fixed-schema)))))

(deftest test-def-fixed-schema-serdes
  (let [data (ba/byte-array [12 24])
        encoded (l/serialize a-fixed-schema data)
        decoded (l/deserialize-same a-fixed-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         data encoded))
    (is (ba/equivalent-byte-arrays? data decoded))))

(deftest test-nested-record-schema
  (let [expected {:name :deercreeklabs.unit.lancaster-test/add-to-cart-rsp
                  :type :record
                  :fields
                  [{:name :qty-requested
                    :type [:null :int]
                    :default nil}
                   {:name :qty-added
                    :type [:null :int]
                    :default nil}
                   {:name :current-qty
                    :type [:null :int]
                    :default nil}
                   {:name :req
                    :type
                    {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                     :type :record
                     :fields [{:name :sku :type :int :default -1}
                              {:name :qty-requested
                               :type [:null :int]
                               :default nil}]}
                    :default {:sku 10
                              :qty-requested nil}}
                   {:name :the-reason-why
                    :type {:name :deercreeklabs.unit.lancaster-test/why
                           :type :enum
                           :symbols [:all :stock :limit]
                           :default :all}
                    :default :stock}
                   {:name :data
                    :type {:name :deercreeklabs.unit.lancaster-test/a-fixed
                           :type :fixed
                           :size 2}
                    :default "MX"}
                   {:name :other-data
                    :type [:null :bytes]
                    :default nil}]}]
    #?(:clj (is (fp-matches? add-to-cart-rsp-schema)))
    (is (= (str
            "{\"name\":\"deercreeklabs.unit.lancaster_test.AddToCartRsp\","
            "\"type\":\"record\",\"fields\":[{\"name\":\"qtyRequested\","
            "\"type\":[\"null\",\"int\"]},{\"name\":\"qtyAdded\",\"type\":"
            "[\"null\",\"int\"]},{\"name\":\"currentQty\",\"type\":[\"null\","
            "\"int\"]},{\"name\":\"req\",\"type\":{\"name\":\"deercreeklabs."
            "unit.lancaster_test.AddToCartReq\",\"type\":\"record\",\"fields\":"
            "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":\"qtyRequested\","
            "\"type\":[\"null\",\"int\"]}]}},{\"name\":\"theReasonWhy\","
            "\"type\":{\"name\":\"deercreeklabs.unit.lancaster_test.Why\","
            "\"type\":\"enum\",\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}},"
            "{\"name\":\"data\",\"type\":{\"name\":\"deercreeklabs.unit."
            "lancaster_test.AFixed\",\"type\":\"fixed\",\"size\":2}},"
            "{\"name\":\"otherData\",\"type\":[\"null\",\"bytes\"]}]}")
           (l/pcf add-to-cart-rsp-schema)))
    (is (= expected (l/edn add-to-cart-rsp-schema))))
  (is (= "3835051015389218749"
         (u/long->str (l/fingerprint64 add-to-cart-rsp-schema)))))

(deftest test-nested-record-serdes
  (let [data {:qty-requested 123
              :qty-added 10
              :current-qty 10
              :req {:sku 123
                    :qty-requested 123}
              :the-reason-why :limit
              :data (ba/byte-array [66 67])
              :other-data (ba/byte-array [123 123])}
        encoded (l/serialize add-to-cart-rsp-schema data)
        decoded (l/deserialize-same add-to-cart-rsp-schema
                                    encoded)]
    (is (= "AvYBAhQCFPYBAvYBBEJDAgR7ew==" (ba/byte-array->b64 encoded)))
    (is (= (xf-byte-arrays data)
           (xf-byte-arrays decoded)))))

(deftest test-null-schema
  (let [data nil
        encoded (l/serialize l/null-schema data)
        decoded (l/deserialize-same l/null-schema encoded)]
    #?(:clj (is (fp-matches? l/null-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array []) encoded))
    (is (= data decoded))))

(deftest test-boolean-schema
  (let [data true
        encoded (l/serialize l/boolean-schema data)
        decoded (l/deserialize-same l/boolean-schema encoded)]
    #?(:clj (is (fp-matches? l/boolean-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [1]) encoded))
    (is (= data decoded))))

(deftest test-int-schema-serdes
  (let [data 7890
        encoded (l/serialize l/int-schema data)
        decoded (l/deserialize-same l/int-schema encoded)]
    #?(:clj (is (fp-matches? l/int-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-92 123]) encoded))
    (is (= data decoded))))

(deftest test-long-schema-serdes
  (let [data (u/ints->long 2147483647 -1)
        encoded (l/serialize l/long-schema data)
        decoded (l/deserialize-same l/long-schema encoded)]
    #?(:clj (is (fp-matches? l/long-schema)))
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
        decoded (l/deserialize-same l/float-schema encoded)
        abs-err (abs-err data decoded)]
    #?(:clj (is (fp-matches? l/float-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-48 15 73 64]) encoded))
    (is (< abs-err 0.000001))))

(deftest test-double-schema
  (let [data (double 3.14159265359)
        encoded (l/serialize l/double-schema data)
        decoded (l/deserialize-same l/double-schema encoded)
        abs-err (abs-err data decoded)]
    #?(:clj (is (fp-matches? l/double-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-22 46 68 84 -5 33 9 64])
                                    encoded))
    (is (< abs-err 0.000001))))

(deftest test-bytes-schema
  (let [data (ba/byte-array [1 1 2 3 5 8 13 21])
        encoded (l/serialize l/bytes-schema data)
        decoded (l/deserialize-same l/bytes-schema encoded)]
    #?(:clj (is (fp-matches? l/bytes-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [16 1 1 2 3 5 8 13 21])
                                    encoded))
    (is (ba/equivalent-byte-arrays? data decoded))))

(deftest test-string-schema
  (let [data "Hello world!"
        encoded (l/serialize l/string-schema data)
        decoded (l/deserialize-same l/string-schema encoded)]
    #?(:clj (is (fp-matches? l/string-schema)))
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [24 72 101 108 108 111 32 119 111 114 108 100 33])
         encoded))
    (is (= data decoded))))

(deftest test-def-map-schema
  (is (= {:type :map :values :int}
         (l/edn ages-schema)))
  #?(:clj (is (fp-matches? ages-schema)))
  (is (= "{\"type\":\"map\",\"values\":\"int\"}"
         (l/pcf ages-schema)))
  (is (= "-2649837581481768589"
         (u/long->str (l/fingerprint64 ages-schema)))))

(deftest test-map-schema-serdes
  (let [data {"Alice" 50
              "Bob" 55
              "Chad" 89}
        encoded (l/serialize ages-schema data)
        decoded (l/deserialize-same ages-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 10 65 108 105 99 101 100 6 66 111 98 110 8
                         67 104 97 100 -78 1 0])
         encoded))
    (is (= data decoded))))

(deftest test-def-array-schema
  #?(:clj (is (fp-matches? simple-array-schema)))
  (is (= {:type :array :items :string}
         (l/edn simple-array-schema)))
  (is (= "{\"type\":\"array\",\"items\":\"string\"}"
         (l/pcf simple-array-schema)))
  (is (= "-3577210133426481249"
         (u/long->str (l/fingerprint64 simple-array-schema)))))

(deftest test-array-schema-serdes
  (let [names ["Ferdinand" "Omar" "Lin"]
        encoded (l/serialize simple-array-schema names)
        decoded (l/deserialize-same simple-array-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 18 70 101 114 100 105 110 97 110 100 8 79
                         109 97 114 6 76 105 110 0])
         encoded))
    (is (= names decoded))))

(deftest test-empty-array-serdes
  (let [sch (l/array-schema l/int-schema)
        data []
        encoded (l/serialize sch data)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0])
               encoded))
        decoded (l/deserialize-same sch encoded)]
    (is (= data decoded))))

(deftest test-nested-array-schema
  #?(:clj (is (fp-matches? rsps-schema)))
  (is (= {:type :array
          :items
          {:name :deercreeklabs.unit.lancaster-test/add-to-cart-rsp
           :type :record
           :fields
           [{:name :qty-requested
             :type [:null :int]
             :default nil}
            {:name :qty-added
             :type [:null :int]
             :default nil}
            {:name :current-qty
             :type [:null :int]
             :default nil}
            {:name :req
             :type {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                    :type :record
                    :fields [{:name :sku
                              :type :int
                              :default -1}
                             {:name :qty-requested
                              :type [:null :int]
                              :default nil}]}
             :default {:sku 10 :qty-requested nil}}
            {:name :the-reason-why
             :type {:name :deercreeklabs.unit.lancaster-test/why
                    :type :enum
                    :symbols [:all :stock :limit]
                    :default :all}
             :default :stock}
            {:name :data
             :type
             {:name :deercreeklabs.unit.lancaster-test/a-fixed
              :type :fixed
              :size 2}
             :default "MX"}
            {:name :other-data
             :type [:null :bytes]
             :default nil}]}}
         (l/edn rsps-schema)))
  (is (= "3719708605096203323"
         (u/long->str (l/fingerprint64 rsps-schema)))))

(deftest test-nested-array-schema-serdes
  (let [data [{:qty-requested 123
               :qty-added 4
               :current-qty 10
               :req
               {:sku 123 :qty-requested 123}
               :the-reason-why :limit
               :data (ba/byte-array [66 67])
               :other-data (ba/byte-array [123 123])}
              {:qty-requested 4
               :qty-added 4
               :current-qty 4
               :req
               {:sku 10 :qty-requested 4}
               :the-reason-why :all
               :data (ba/byte-array [100 110])
               :other-data (ba/byte-array [64 74])}]
        encoded (l/serialize rsps-schema data)
        decoded (l/deserialize-same rsps-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [4 2 246 1 2 8 2 20 246 1 2 246 1 4 66 67 2 4 123 123
                         2 8 2 8 2 8 20 2 8 0 100 110 2 4 64 74 0])
         encoded))
    (is (= (xf-byte-arrays data)
           (xf-byte-arrays decoded)))))

(deftest test-nested-map-schema
  #?(:clj (is (fp-matches? nested-map-schema)))
  (is (= {:type :map
          :values
          {:name :deercreeklabs.unit.lancaster-test/add-to-cart-rsp
           :type :record
           :fields
           [{:name :qty-requested :type [:null :int] :default nil}
            {:name :qty-added :type [:null :int] :default nil}
            {:name :current-qty :type [:null :int] :default nil}
            {:name :req
             :type {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                    :type :record
                    :fields [{:name :sku :type :int :default -1}
                             {:name :qty-requested
                              :type [:null :int]
                              :default nil}]}
             :default {:sku 10 :qty-requested nil}}
            {:name :the-reason-why
             :type {:name :deercreeklabs.unit.lancaster-test/why
                    :type :enum
                    :symbols [:all :stock :limit]
                    :default :all}
             :default :stock}
            {:name :data
             :type
             {:name :deercreeklabs.unit.lancaster-test/a-fixed
              :type :fixed
              :size 2}
             :default "MX"}
            {:name :other-data
             :type [:null :bytes]
             :default nil}]}}
         (l/edn nested-map-schema)))
  (is (= "-6484319793187085262"
         (u/long->str (l/fingerprint64 nested-map-schema)))))

(deftest test-nested-map-schema-serdes
  (let [data {"A" {:qty-requested 123
                   :qty-added 4
                   :current-qty 10
                   :req {:sku 123
                         :qty-requested 123}
                   :the-reason-why :limit
                   :data (ba/byte-array [66 67])
                   :other-data (ba/byte-array [123 123])}
              "B" {:qty-requested 4
                   :qty-added 4
                   :current-qty 4
                   :req {:sku 10
                         :qty-requested 4}
                   :the-reason-why :all
                   :data (ba/byte-array [100 110])
                   :other-data (ba/byte-array [64 74])}}
        encoded (l/serialize nested-map-schema data)
        decoded (l/deserialize-same nested-map-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [4 2 65 2 246 1 2 8 2 20 246 1 2 246 1 4 66 67 2 4 123
                         123 2 66 2 8 2 8 2 8 20 2 8 0 100 110 2 4 64 74 0])
         encoded))
    (is (= (xf-byte-arrays data)
           (xf-byte-arrays decoded)))))

(deftest test-empty-map-serdes
  (let [sch (l/map-schema l/int-schema)
        data {}
        encoded (l/serialize sch data)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0])
               encoded))
        decoded (l/deserialize-same sch encoded)]
    (is (= data decoded))))

(deftest test-union-schema
  #?(:clj (is (fp-matches? union-schema)))
  (is (= [:int
          {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
           :type :record
           :fields [{:name :sku
                     :type :int
                     :default -1}
                    {:name :qty-requested
                     :type [:null :int]
                     :default nil}]}
          {:name :deercreeklabs.unit.lancaster-test/a-fixed
           :type :fixed
           :size 2}]
         (l/edn union-schema)))
  (is (= "-193173046069528093"
         (u/long->str (l/fingerprint64 union-schema)))))

(deftest test-union-schema-serdes
  (let [data {:sku 123 :qty-requested 4}
        encoded (l/serialize union-schema data)
        decoded (l/deserialize-same union-schema encoded)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [2 246 1 2 8])
                                          encoded))
        _ (is (= data decoded))
        data 5
        encoded (l/serialize union-schema data)
        decoded (l/deserialize-same union-schema encoded)]
    (is (ba/equivalent-byte-arrays? (ba/byte-array [0 10])
                                    encoded))
    (is (= data decoded))))

(deftest test-union-schema-w-multiple-records
  #?(:clj (is (fp-matches? person-or-dog-schema)))
  (is (= [{:name :deercreeklabs.unit.lancaster-test/person
           :type :record
           :fields
           [{:name :person-name :type :string :default "No name"}
            {:name :age :type :int :default 0}]}
          {:name :deercreeklabs.unit.lancaster-test/dog
           :type :record
           :fields
           [{:name :name :type [:null :string] :default nil}
            {:name :owner :type [:null :string] :default nil}]}]
         (l/edn person-or-dog-schema)))
  (is (= "4086940842367520063"
         (u/long->str (l/fingerprint64 person-or-dog-schema)))))

(deftest test-multi-record-union-schema-serdes
  (let [data {:name "Fido" :owner "Zach"}
        encoded (l/serialize person-or-dog-schema data)
        decoded (l/deserialize-same person-or-dog-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [2 2 8 70 105 100 111 2 8 90 97 99 104])
               encoded))
        _ (is (= data decoded))
        data {:person-name "Bill" :age 50}
        encoded (l/serialize person-or-dog-schema data)
        decoded (l/deserialize-same person-or-dog-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [0 8 66 105 108 108 100])
         encoded))
    (is (= data decoded))))

(deftest test-multi-record-schema-serdes-non-existent
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not match any schema in the given union schema"
       (l/serialize person-or-dog-schema {:k 1}))))

(deftest test-map-or-array-schema
  #?(:clj (is (fp-matches? map-or-array-schema)))
  (is (= [{:type :map :values :int}
          {:type :array :items :string}]
         (l/edn map-or-array-schema)))
  (is (= "4441440791563688855"
         (u/long->str (l/fingerprint64 map-or-array-schema)))))

(deftest test-map-or-array-schema-serdes
  (let [data {"Zeke" 22 "Adeline" 88}
        encoded (l/serialize map-or-array-schema data)
        decoded (l/deserialize-same map-or-array-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0 4 8 90 101 107 101 44 14 65 100 101 108 105
                               110 101 -80 1 0])
               encoded))
        _ (is (= data decoded))
        data ["a thing" "another thing"]
        encoded (l/serialize map-or-array-schema data)
        decoded (l/deserialize-same map-or-array-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [2 4 14 97 32 116 104 105 110 103 26 97 110
                         111 116 104 101 114 32 116 104 105 110 103 0])
         encoded))
    (is (= data decoded))))

(deftest test-mopodoa-schema
  #?(:clj (is (fp-matches? mopodoa-schema)))
  (is (= [{:type :map :values :int}
          {:name :deercreeklabs.unit.lancaster-test/person
           :type :record
           :fields [{:name :person-name
                     :type :string
                     :default "No name"}
                    {:name :age
                     :type :int
                     :default 0}]}
          {:name :deercreeklabs.unit.lancaster-test/dog
           :type :record
           :fields [{:name :name
                     :type [:null :string]
                     :default nil}
                    {:name :owner
                     :type [:null :string]
                     :default nil}]}
          {:type :array :items :string}]
         (l/edn mopodoa-schema)))
  (is (= "6581003250645933229"
         (u/long->str (l/fingerprint64 mopodoa-schema)))))

(deftest test-mopodoa-schema-serdes
  (let [data {"Zeke" 22 "Adeline" 88}
        encoded (l/serialize mopodoa-schema data)
        decoded (l/deserialize-same mopodoa-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0 4 8 90 101 107 101 44 14 65 100 101 108 105
                               110 101 -80 1 0])
               encoded))
        _ (is (= data decoded))
        data ["a thing" "another thing"]
        encoded (l/serialize mopodoa-schema data)
        decoded (l/deserialize-same mopodoa-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 4 14 97 32 116 104 105 110 103 26 97 110
                         111 116 104 101 114 32 116 104 105 110 103 0])
         encoded))
    (is (= data decoded))))

(deftest test-recursive-schema
  (is (= {:name :deercreeklabs.unit.lancaster-test/tree
          :type :record
          :fields
          [{:name :value :type :int :default -1}
           {:name :right
            :type [:null :tree]
            :default nil}
           {:name :left
            :type [:null :tree]
            :default nil}]}
         (l/edn tree-schema)))
  #?(:clj (is (fp-matches? tree-schema)))
  (is (= "-3297333764539234889"
         (u/long->str (l/fingerprint64 tree-schema)))))

(deftest test-edn-schemas-match?-recursive-schema
  (is (u/edn-schemas-match? (l/edn tree-schema) (l/edn tree-schema) {} {})))

(deftest test-recursive-schema-serdes
  (let [data {:value 5
              :right {:value -10
                      :right {:value -20}}
              :left {:value 10
                     :left {:value 20
                            :left {:value 40}}}}
        encoded (l/serialize tree-schema data)
        decoded (l/deserialize-same tree-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [10 2 19 2 39 0 0 0 2 20 0 2 40 0 2 80 0 0])
         encoded))
    (is (= data decoded))))

(deftest test-rec-w-array-and-enum-schema
  #?(:clj (is (fp-matches? rec-w-array-and-enum-schema)))
  (is (= {:name :deercreeklabs.unit.lancaster-test/rec-w-array-and-enum
          :type :record
          :fields
          [{:name :names
            :type {:type :array :items :string}
            :default []}
           {:name :why
            :type [:null
                   {:default :all
                    :name :deercreeklabs.unit.lancaster-test/why
                    :symbols [:all :stock :limit]
                    :type :enum}]
            :default nil}]}
         (l/edn rec-w-array-and-enum-schema)))
  (is (= "-7358640627267953104"
         (u/long->str (l/fingerprint64
                       rec-w-array-and-enum-schema)))))

(deftest test-rec-w-array-and-enum-serdes
  (let [data {:names ["Aria" "Beth" "Cindy"]
              :why :stock}
        encoded (l/serialize rec-w-array-and-enum-schema data)
        decoded (l/deserialize-same rec-w-array-and-enum-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 8 65 114 105 97 8 66 101 116 104 10 67 105 110 100
                         121 0 2 2])
         encoded))
    (is (= data decoded))))

(deftest test-rec-w-map-schema
  #?(:clj (is (fp-matches? rec-w-map-schema)))
  (is (= {:name :deercreeklabs.unit.lancaster-test/rec-w-map
          :type :record
          :fields [{:name :name-to-age
                    :type [:null {:type :map
                                  :values :int}]
                    :default nil}
                   {:name :what
                    :type [:null :string]
                    :default nil}]}
         (l/edn rec-w-map-schema)))
  (is (= "1679652566194036700"
         (u/long->str (l/fingerprint64
                       rec-w-map-schema)))))

(deftest test-rec-w-map-serdes
  (let [data {:name-to-age {"Aria" 22
                            "Beth" 33
                            "Cindy" 44}
              :what "yo"}
        encoded (l/serialize rec-w-map-schema data)
        decoded (l/deserialize-same rec-w-map-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [2 6 8 65 114 105 97 44 8 66 101 116 104 66 10 67 105
                         110 100 121 88 0 2 4 121 111])
         encoded))
    (is (= data decoded))))

(deftest test-rec-w-fixed-no-default
  #?(:clj (is (fp-matches? rec-w-fixed-no-default-schema)))
  (is (= {:name :deercreeklabs.unit.lancaster-test/rec-w-fixed-no-default
          :type :record
          :fields
          [{:name :data
            :type
            {:name :deercreeklabs.unit.lancaster-test/a-fixed
             :type :fixed
             :size 2}
            :default "\0\0"}]}
         (l/edn rec-w-fixed-no-default-schema)))
  (is (= "-6875395607105571061"
         (u/long->str (l/fingerprint64
                       rec-w-fixed-no-default-schema)))))

(deftest test-rec-w-fixed-no-default-serdes
  (let [data {:data (ba/byte-array [1 2])}
        encoded (l/serialize rec-w-fixed-no-default-schema data)
        decoded (l/deserialize-same rec-w-fixed-no-default-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [1 2])
         encoded))
    (is (ba/equivalent-byte-arrays?
         (:data data)
         (:data decoded)))))

(deftest test-rec-w-maybe-field
  #?(:clj (is (fp-matches? rec-w-maybe-field-schema)))
  (is (= {:name :deercreeklabs.unit.lancaster-test/rec-w-maybe-field
          :type :record
          :fields [{:name :name
                    :type [:null :string]
                    :default nil}
                   {:name :age
                    :type [:null :int]
                    :default nil}]}
         (l/edn rec-w-maybe-field-schema)))
  (is (= "-5686522258470805846"
         (u/long->str (l/fingerprint64 rec-w-maybe-field-schema))))
  (is (= "8614342a3c7b1d979361bb2f91e684e3"
         (-> (l/fingerprint128 rec-w-maybe-field-schema)
             (ba/byte-array->hex-str))))
  (is (= "ca448fc66bedd19450250f871fe26a2859a227db1ca5ceac1f632d58528f17a9"
         (-> (l/fingerprint256 rec-w-maybe-field-schema)
             (ba/byte-array->hex-str)))))

(deftest test-record-serdes-missing-field
  (let [data {:qty-requested 100}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:sku`"
         (l/serialize add-to-cart-req-schema data)))))

(deftest test-record-serdes-missing-maybe-field
  (let [data {:name "Sharon"}
        encoded (l/serialize rec-w-maybe-field-schema data)
        decoded (l/deserialize-same rec-w-maybe-field-schema encoded)]
    (is (= data decoded))))

(deftest test-schema?
  (is (l/schema? person-or-dog-schema))
  (is (not (l/schema? :foo))))

(deftest test-bad-serialize-arg
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument to `serialize` must be a schema object"
       (l/serialize nil nil))))

(deftest test-bad-deserialize-args
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument to `deserialize` must be a schema object"
       (l/deserialize nil nil nil)))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Second argument to `deserialize` must be a "
       (l/deserialize why-schema nil (ba/byte-array []))))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"argument to `deserialize` must be a byte array"
       (l/deserialize why-schema why-schema []))))

(deftest test-field-default-validation
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Bad default value for field `:int-field`. Got `a`."
       (l/record-schema :test-schema
                        [[:int-field l/int-schema "a"]]))))

(deftest test-default-data
  (is (= :all (l/default-data why-schema)))
  (is (= {:sku -1
          :qty-requested nil}
         (l/default-data add-to-cart-req-schema))))

(deftest test-bad-field-name
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Name keywords must start with a letter and subsequently"
       (l/record-schema :test-schema
                        [[:bad? l/boolean-schema]]))))

(deftest test-bad-record-name
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Name keywords must start with a letter and subsequently"
       (l/record-schema :*test-schema*
                        [[:is-good l/boolean-schema]]))))

(deftest test-duplicate-field-name
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Field names must be unique."
       (l/record-schema :test-schema
                        [[:int-field l/int-schema]
                         [:int-field l/int-schema]]))))

(deftest test-good-union
  (let [sch1 (l/record-schema ::sch1 [[:a l/int-schema]
                                      [:b l/string-schema]])
        sch2 (l/record-schema ::sch2 [[:different-b l/string-schema]])
        sch3 (l/union-schema [sch1 sch2])]
    (is (round-trip? sch3 {:different-b "hi"}))
    (is (round-trip? sch3 {:a 1 :b "hi"}))))

(deftest test-serialize-bad-union-member
  (let [schema (l/union-schema [l/null-schema l/int-schema])]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"does not match any schema in the given union schema"
         (l/serialize schema :foo)))))

(deftest test-maybe-w-union-arg
  (let [schema-1 (l/maybe (l/union-schema [l/int-schema l/string-schema]))
        schema-2 (l/maybe (l/union-schema [l/null-schema l/int-schema]))]
    (is (round-trip? schema-1 nil))
    (is (round-trip? schema-2 nil))
    (is (round-trip? schema-1 34))
    (is (round-trip? schema-2 34))))

(deftest test-more-than-one-numeric-type-in-a-union
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Unions may not contain more than one numeric schema"
       (l/union-schema [l/int-schema l/float-schema]))))

(deftest test-more-than-one-bytes-type-in-a-union
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Unions may not contain more than one byte-array schema"
       (l/union-schema [l/bytes-schema (l/fixed-schema :foo 16)]))))

(deftest test-serialize-empty-map-multi-rec-union
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Can't serialize data `\{\}` because it matches multiple record schemas"
       (l/serialize person-or-dog-schema {}))))

(deftest test-map-and-rec-union
  (let [schema (l/union-schema [(l/map-schema l/int-schema) person-schema])]
    (is (round-trip? schema {"foo" 1}))
    (is (round-trip? schema {:person-name "Chad" :age 18}))))

(deftest test-ns-enum
  (is (round-trip? suit-schema :spades))
  (is (= {:name :deercreeklabs.unit.lancaster-test/suit
          :type :enum
          :symbols [:hearts :clubs :spades :diamonds]
          :default :hearts}
         (l/edn suit-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.Suit\",\"type\":"
              "\"enum\",\"symbols\":[\"HEARTS\",\"CLUBS\",\"SPADES\","
              "\"DIAMONDS\"],\"default\":\"HEARTS\"}")
         (l/json suit-schema))))

(deftest test-identical-schemas-in-union
  (let [sch1 (l/enum-schema ::a-name [:a :b])
        sch2 (l/enum-schema ::a-name [:a :b])]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Identical schemas in union"
         (l/union-schema [l/string-schema sch1 sch2])))))

(deftest test-schemas-match?
  (let [sch1 (l/enum-schema ::a-name [:a :b])
        sch2 (l/enum-schema ::a-name [:a :b])
        sch3 (l/enum-schema ::x-name [:a :b])]
    (is (l/schemas-match? sch1 sch2))
    (is (not (l/schemas-match? sch1 sch3)))))

(deftest test-missing-record-field
  (let [sch1 (l/record-schema ::test [[:a :required l/int-schema]])
        sch2 (l/union-schema [l/int-schema sch1])
        sch3 (l/record-schema ::sch3 [[:arg sch2]])
        sch4 (l/array-schema sch3)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:a`"
         (l/serialize sch1 {:b 1})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:a`"
         (l/serialize sch2 {:b 1})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:a`"
         (l/serialize sch3 {:arg {:b 1}})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:a`"
         (l/serialize sch4 [{:arg {:a 1}}
                            {:arg {:b 1}}])))))

(deftest serialize-small-double-into-float-union
  (let [sch (l/union-schema [l/null-schema l/float-schema])
        v (double 6.0)
        encoded (l/serialize sch v)
        decoded (l/deserialize-same sch encoded)]
    (is (= v decoded))))

(deftest serialize-long-into-union
  (let [sch (l/union-schema [l/null-schema l/long-schema])
        v (u/str->long "-5442038735385698765")
        encoded (l/serialize sch v)
        decoded (l/deserialize-same sch encoded)]
    (is (= v decoded))))

(deftest serialize-lazy-sequence-into-union
  (let [sch (l/union-schema [l/null-schema (l/array-schema l/int-schema)])
        v (map identity [42 681 1024]) ; Make a lazy seq
        encoded (l/serialize sch v)
        decoded (l/deserialize-same sch encoded)]
    (is (= v decoded))))

(deftest test-rt-set
  (let [sch l/string-set-schema
        v #{"a" "b" "1234" "c45"}
        encoded (l/serialize sch v)
        decoded (l/deserialize-same sch encoded)]
    (is (= v decoded))))

(deftest test-rt-set-record-field
  (let [sch (l/record-schema ::a-rec
                             [[:the-set l/string-set-schema]
                              [:the-string l/string-schema]])
        v {:the-set #{"a" "b" "1234" "c45"}
           :the-string "yo"}
        encoded (l/serialize sch v)
        decoded (l/deserialize-same sch encoded)]
    (is (= v decoded))))

(deftest test-bad-set-type
  (let [sch l/string-set-schema
        v #{"2" :k}] ;; not strictly string members
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Set element `:k` .* is not a valid string"
         (l/serialize sch v)))))

(deftest test-bad-set-type-map-of-nils
  (let [sch (l/map-schema l/null-schema) ;; equivalent to l/string-set-schema
        v {"a" nil "b" nil}] ;; Must be a set
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"is not a valid Clojure set"
         (l/serialize sch v)))))

(deftest test-serialize-map-instead-of-sequence
  (let [sch (l/record-schema ::test-record
                             [[:items (l/array-schema l/string-schema)]])
        v {:items {0 "a"}}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"does not match any schema in the given union schema"
         (l/serialize sch v)))))

(deftest test-child-schema-rec-w-no-namespace
  (let [sch (l/record-schema :test-schema ; note no namespace
                             [[:a l/boolean-schema]])
        child-edn (-> (l/child-schema sch :a)
                      (l/edn))]
    (is (= [:null :boolean] child-edn))))

(deftest test-namespaced-field-name
  ;; Namespaces are not allowed on field names as per
  ;; https://avro.apache.org/docs/1.11.1/specification/#names
  ;; "Record fields and enum symbols have names as well (but no namespace)."
  (is (thrown-with-msg?
       ExceptionInfo
       #"Field name keywords must not be namespaced"
       (l/record-schema ::example [[::id l/int-schema]]))))

(deftest test-namespaced-enum-symbol
  ;; Namespaces are not allowed on enum symbols as per
  ;; https://avro.apache.org/docs/1.11.1/specification/#names
  ;; "Record fields and enum symbols have names as well (but no namespace)."
  (is (thrown-with-msg?
       ExceptionInfo
       #"Enum symbol keywords must not be namespaced"
       (l/enum-schema ::my-enum [:my-ns/sym-1 :my-ns/sym-2]))))

(deftest test-ambiguous-union
  (let [a-sch (l/record-schema :a [[:x l/int-schema]
                                   [:y l/string-schema]])
        b-sch (l/record-schema :b [[:x l/int-schema]
                                   [:z l/string-schema]])
        union-sch (l/union-schema [a-sch b-sch])
        data-a {::l/record-name :a
                :y "Hi"}
        encoded-a (l/serialize union-sch data-a)
        decoded-a (l/deserialize-same union-sch encoded-a)
        _ (is (= data-a decoded-a))
        data-b {::l/record-name :b
                :z "Hello"}
        encoded-b (l/serialize union-sch data-b)
        decoded-b (l/deserialize-same union-sch encoded-b)
        _ (is (= data-b decoded-b))
        data-c {::l/record-name :b
                :x 45
                :z "Hi"}
        encoded-c (l/serialize union-sch data-c)
        decoded-c (l/deserialize-same union-sch encoded-c
                                      {:add-record-name :never})]
    (is (= (dissoc data-c ::l/record-name)
           decoded-c))
    (is (thrown-with-msg?
         ExceptionInfo
         #"Missing `:deercreeklabs.lancaster/record-name` value in record data"
         (l/serialize union-sch {:x 42})))
    (is (thrown-with-msg?
         ExceptionInfo
         #"The given record name `:not-real` is not a valid name for any"
         (l/serialize union-sch {::l/record-name :not-real
                                 :x 42})))))

;; TODO: Test union of :null and :null. (multiples of any non-named type
;; are illegal according to the avro spec)
