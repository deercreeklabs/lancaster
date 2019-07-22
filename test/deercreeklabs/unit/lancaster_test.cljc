(ns deercreeklabs.unit.lancaster-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.walk :as walk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.pcf-utils :as pcf-utils]
   [deercreeklabs.lancaster.utils :as u]
   [schema.core :as s :include-macros true])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo)
      (org.apache.avro Schema
                       SchemaNormalization
                       Schema$Parser))))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

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
  [:sku l/int-schema]
  [:qty-requested l/int-schema 0])

(def add-to-cart-req-v2-schema
  (l/record-schema ::add-to-cart-req
                   [[:sku l/int-schema]
                    [:qty-requested l/int-schema 0]
                    [:note l/string-schema "No note"]]))

(def add-to-cart-req-v3-schema ;; qtys are floats!
  (l/record-schema ::add-to-cart-req
                   [[:sku l/int-schema]
                    [:qty-requested l/float-schema]
                    [:note l/string-schema "No note"]]))

(def add-to-cart-req-v4-schema
  (l/record-schema ::add-to-cart-req
                   [[:sku l/int-schema]
                    [:qty-requested l/int-schema]
                    [:comment l/string-schema]]))

(l/def-enum-schema why-schema
  :all :stock :limit)

(l/def-enum-schema suit-schema
  :suit/hearts :suit/clubs :suit/spades :suit/diamonds)

(l/def-fixed-schema a-fixed-schema
  2)

(l/def-maybe-schema maybe-int-schema
  l/int-schema)

(l/def-record-schema rec-w-maybe-field-schema
  [:name l/string-schema]
  [:age maybe-int-schema])

(l/def-record-schema rec-w-fixed-no-default-schema
  [:data a-fixed-schema])

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested l/int-schema]
  [:qty-added l/int-schema]
  [:current-qty l/int-schema]
  [:req add-to-cart-req-schema {:sku 10 :qty-requested 1}]
  [:the-reason-why why-schema :stock]
  [:data a-fixed-schema (ba/byte-array [77 88])]
  [:other-data l/bytes-schema])

(l/def-array-schema simple-array-schema
  l/string-schema)

(l/def-array-schema rsps-schema
  add-to-cart-rsp-schema)

(l/def-record-schema rec-w-array-and-enum-schema
  [:names simple-array-schema]
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
  [:person/name l/string-schema "No name"]
  [:person/age l/int-schema 0])

(l/def-record-schema dog-schema
  [:dog/name l/string-schema "No name"]
  [:dog/owner l/string-schema "No owner"])

(def dog-v2-schema
  (l/record-schema ::dog
                   [[:dog/name l/string-schema "No name"]
                    [:dog/owner l/string-schema "No owner"]
                    [:dog/tag-number l/int-schema]]))

(l/def-record-schema fish-schema
  [:fish/name l/string-schema "No name"]
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
  [:value l/int-schema]
  [:right (l/maybe :tree)]
  [:left (l/maybe :tree)])

(deftest test-record-schema
  (let [expected-pcf (str "{\"name\":\"deercreeklabs.unit.lancaster_test."
                          "AddToCartReq\",\"type\":\"record\",\"fields\":"
                          "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                          "\"qtyRequested\",\"type\":\"int\"}]}")
        expected-edn {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                      :type :record
                      :fields
                      [{:name :sku
                        :type :int
                        :default -1}
                       {:name :qty-requested
                        :type :int
                        :default 0}]}]
    #?(:clj (is (fp-matches? add-to-cart-req-schema)))
    (is (= "-6098958998487321663"
           (u/long->str (l/fingerprint64 add-to-cart-req-schema))))
    (is (= expected-edn (l/edn add-to-cart-req-schema)))
    (is (= expected-pcf (l/pcf
                         add-to-cart-req-schema)))))

(deftest test-def-record-schema-serdes
  (let [data {:sku 123
              :qty-requested 5}
        encoded (l/serialize add-to-cart-req-schema data)
        decoded (l/deserialize-same add-to-cart-req-schema encoded)]
    (is (= "9gEK" (ba/byte-array->b64 encoded)))
    (is (= data decoded))))

(deftest test-def-record-schema-mixed-ns
  (is (= {:name :deercreeklabs.unit.lancaster-test/fish
          :type :record
          :fields [{:name :fish/name
                    :type :string
                    :default "No name"}
                   {:name :tank-num
                    :type :int
                    :default -1}]}
         (l/edn fish-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.Fish\",\"type\":"
              "\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\","
              "\"default\":\"No name\"},{\"name\":\"tankNum\",\"type\":"
              "\"int\",\"default\":-1}]}")
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
                  [{:name :qty-requested :type :int :default -1}
                   {:name :qty-added :type :int :default -1}
                   {:name :current-qty :type :int :default -1}
                   {:name :req
                    :type
                    {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                     :type :record
                     :fields [{:name :sku :type :int :default -1}
                              {:name :qty-requested
                               :type :int
                               :default 0}]}
                    :default {:sku 10 :qty-requested 1}}
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
                    :type :bytes
                    :default ""}]}]
    #?(:clj (is (fp-matches? add-to-cart-rsp-schema)))
    (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.AddToCartRsp\","
                "\"type\":\"record\",\"fields\":[{\"name\":\"qtyRequested\","
                "\"type\":\"int\"},{\"name\":\"qtyAdded\",\"type\":\"int\"},"
                "{\"name\":\"currentQty\",\"type\":\"int\"},{\"name\":\"req\","
                "\"type\":{\"name\":\"deercreeklabs.unit.lancaster_test."
                "AddToCartReq\",\"type\":\"record\",\"fields\":[{\"name\":"
                "\"sku\",\"type\":\"int\"},{\"name\":\"qtyRequested\",\"type"
                "\":\"int\"}]}},{\"name\":\"theReasonWhy\",\"type\":{\"name\":"
                "\"deercreeklabs.unit.lancaster_test.Why\",\"type\":\"enum\","
                "\"symbols\":[\"ALL\",\"STOCK\",\"LIMIT\"]}},{\"name\":"
                "\"data\",\"type\":{\"name\":\"deercreeklabs.unit.lancaster_test."
                "AFixed\",\"type\":\"fixed\",\"size\":2}},{\"name\":"
                "\"otherData\",\"type\":\"bytes\"}]}")
           (l/pcf add-to-cart-rsp-schema)))
    (is (= expected (l/edn add-to-cart-rsp-schema))))
  (is (= "-4997526840462835541"
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
    (is (= "9gEUFPYB9gEEQkMEe3s=" (ba/byte-array->b64 encoded)))
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
           [{:name :qty-requested :type :int :default -1}
            {:name :qty-added :type :int :default -1}
            {:name :current-qty :type :int :default -1}
            {:name :req
             :type {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                    :type :record
                    :fields
                    [{:name :sku :type :int :default -1}
                     {:name :qty-requested :type :int :default 0}]}
             :default {:sku 10 :qty-requested 1}}
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
            {:name :other-data :type :bytes :default ""}]}}
         (l/edn rsps-schema)))
  (is (= "4121614369257759480"
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
         (ba/byte-array [4 -10 1 8 20 -10 1 -10 1 4 66 67 4 123 123
                         8 8 8 20 8 0 100 110 4 64 74 0])
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
           [{:name :qty-requested :type :int :default -1}
            {:name :qty-added :type :int :default -1}
            {:name :current-qty :type :int :default -1}
            {:name :req
             :type {:name :deercreeklabs.unit.lancaster-test/add-to-cart-req
                    :type :record
                    :fields [{:name :sku :type :int :default -1}
                             {:name :qty-requested :type :int :default 0}]}
             :default {:sku 10 :qty-requested 1}}
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
            {:name :other-data :type :bytes :default ""}]}}
         (l/edn nested-map-schema)))
  (is (= "550517813727580798"
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
         (ba/byte-array [4 2 65 -10 1 8 20 -10 1 -10 1 4 66 67 4
                         123 123 2 66 8 8 8 20 8 0 100 110 4 64 74 0])
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
           :fields
           [{:name :sku :type :int :default -1}
            {:name :qty-requested :type :int :default 0}]}
          {:name :deercreeklabs.unit.lancaster-test/a-fixed
           :type :fixed
           :size 2}]
         (l/edn union-schema)))
  (is (= "220769290814327109"
         (u/long->str (l/fingerprint64 union-schema)))))

(deftest test-union-schema-serdes
  (let [data {:sku 123 :qty-requested 4}
        encoded (l/serialize union-schema data)
        decoded (l/deserialize-same union-schema encoded)
        _ (is (ba/equivalent-byte-arrays? (ba/byte-array [2 -10 1 8])
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
           [{:name :person/name :type :string :default "No name"}
            {:name :person/age :type :int :default 0}]}
          {:name :deercreeklabs.unit.lancaster-test/dog
           :type :record
           :fields
           [{:name :dog/name :type :string :default "No name"}
            {:name :dog/owner :type :string :default "No owner"}]}]
         (l/edn person-or-dog-schema)))
  (is (= "4442216514942460391"
         (u/long->str (l/fingerprint64 person-or-dog-schema)))))

(deftest test-multi-record-union-schema-serdes
  (let [data #:dog{:name "Fido" :owner "Zach"}
        encoded (l/serialize person-or-dog-schema data)
        decoded (l/deserialize-same person-or-dog-schema encoded)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [2 8 70 105 100 111 8 90 97 99 104])
               encoded))
        _ (is (= data decoded))
        data #:person{:name "Bill" :age 50}
        encoded (l/serialize person-or-dog-schema data)
        decoded (l/deserialize-same person-or-dog-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [0 8 66 105 108 108 100])
         encoded))
    (is (= data decoded))))

(deftest test-multi-record-schema-serdes-non-existent
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not match any schema in the union schema"
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
           :fields [{:name :person/name
                     :type :string
                     :default "No name"}
                    {:name :person/age
                     :type :int
                     :default 0}]}
          {:name :deercreeklabs.unit.lancaster-test/dog
           :type :record
           :fields [{:name :dog/name
                     :type :string
                     :default "No name"}
                    {:name :dog/owner
                     :type :string
                     :default "No owner"}]}
          {:type :array :items :string}]
         (l/edn mopodoa-schema)))
  (is (= "274497413942050586"
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
  (is (u/edn-schemas-match? (l/edn tree-schema) (l/edn tree-schema) {})))

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
            :type {:name :deercreeklabs.unit.lancaster-test/why
                   :type :enum
                   :symbols [:all :stock :limit]
                   :default :all}
            :default :all}]}
         (l/edn rec-w-array-and-enum-schema)))
  (is (= "-513897484330170713"
         (u/long->str (l/fingerprint64
                       rec-w-array-and-enum-schema)))))

(deftest test-rec-w-array-and-enum-serdes
  (let [data {:names ["Aria" "Beth" "Cindy"]
              :why :stock}
        encoded (l/serialize rec-w-array-and-enum-schema data)
        decoded (l/deserialize-same rec-w-array-and-enum-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6 8 65 114 105 97 8 66 101 116 104 10 67 105
                         110 100 121 0 2])
         encoded))
    (is (= data decoded))))

(deftest test-rec-w-map-schema
  #?(:clj (is (fp-matches? rec-w-map-schema)))
  (is (= {:name :deercreeklabs.unit.lancaster-test/rec-w-map
          :type :record
          :fields [{:name :name-to-age
                    :type {:type :map :values :int}
                    :default {}}
                   {:name :what :type :string :default ""}]}
         (l/edn rec-w-map-schema)))
  (is (= "-2363848852859553430"
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
         (ba/byte-array [6 8 65 114 105 97 44 8 66 101 116 104 66 10
                         67 105 110 100 121 88 0 4 121 111])
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
  (is (= {:name :deercreeklabs.unit.lancaster-test/rec-w-maybe-field,
          :type :record,
          :fields
          [{:name :name, :type :string, :default ""}
           {:name :age, :type [:null :int], :default nil}]}
         (l/edn rec-w-maybe-field-schema)))
  (is (= "6155099556329734528"
         (u/long->str (l/fingerprint64 rec-w-maybe-field-schema)))))

(deftest test-record-serdes-missing-field
  (let [data {:sku 100}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Record data is missing key `:qty-requested`"
         (l/serialize add-to-cart-req-schema data)))))

(deftest test-record-serdes-missing-maybe-field
  (let [data {:name "Sharon"}
        encoded (l/serialize rec-w-maybe-field-schema data)
        decoded (l/deserialize-same rec-w-maybe-field-schema encoded)]
    (is (= (assoc data :age nil) decoded))))

(deftest test-schema?
  (is (l/schema? person-or-dog-schema))
  (is (not (l/schema? :foo))))

(deftest test-bad-serialize-arg
  (s/without-fn-validation ;; Allow built-in handlers to throw
   (is (thrown-with-msg?
        #?(:clj ExceptionInfo :cljs js/Error)
        #"First argument to serialize must be a schema object"
        (l/serialize nil nil)))))

(deftest test-bad-deserialize-args
  (s/without-fn-validation ;; Allow built-in handlers to throw
   (is (thrown-with-msg?
        #?(:clj ExceptionInfo :cljs js/Error)
        #"First argument to deserialize must be a schema object"
        (l/deserialize nil nil nil)))
   (is (thrown-with-msg?
        #?(:clj ExceptionInfo :cljs js/Error)
        #"Second argument to deserialize must be a "
        (l/deserialize why-schema nil (ba/byte-array []))))
   (is (thrown-with-msg?
        #?(:clj ExceptionInfo :cljs js/Error)
        #"argument to deserialize must be a byte array"
        (l/deserialize why-schema why-schema [])))))

(deftest test-field-default-validation
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Default value for field .* is invalid"
       (l/record-schema :test-schema
                        [[:int-field l/int-schema "a"]]))))

(deftest test-default-data
  (is (= :all (l/default-data why-schema)))
  (is (= {:sku -1, :qty-requested 0}
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
        sch2 (l/record-schema ::sch2 [[:different-ns/b l/string-schema]])
        sch3 (l/union-schema [sch1 sch2])]
    (is (round-trip? sch3 {:different-ns/b "hi"}))
    (is (round-trip? sch3 {:a 1 :b "hi"}))))

(deftest test-bad-union
  (let [sch1 (l/record-schema ::sch1 [[:a l/int-schema]
                                      [:b l/string-schema]])
        sch2 (l/record-schema ::sch2 [[:b l/string-schema]])]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Ambiguous union"
         (l/union-schema [sch1 sch2])))))

(deftest test-serialize-bad-union-member
  (let [schema (l/union-schema [l/null-schema l/int-schema])]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"does not match any schema in the union schema"
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
       #"Record data is missing key `:person/name`"
       (l/serialize person-or-dog-schema {}))))

(deftest test-map-and-rec-union
  (let [schema (l/union-schema [(l/map-schema l/int-schema) person-schema])]
    (is (round-trip? schema {"foo" 1}))
    (is (round-trip? schema #:person{:name "Chad" :age 18}))))

(deftest test-ns-enum
  (is (round-trip? suit-schema :suit/spades))
  (is (= {:name :deercreeklabs.unit.lancaster-test/suit
          :type :enum
          :symbols [:suit/hearts :suit/clubs :suit/spades :suit/diamonds]
          :default :suit/hearts}
         (l/edn suit-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.lancaster_test.Suit\",\"type\":"
              "\"enum\",\"symbols\":[\"HEARTS\",\"CLUBS\",\"SPADES\","
              "\"DIAMONDS\"],\"default\":\"HEARTS\"}")
         (l/json suit-schema))))
