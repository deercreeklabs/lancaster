(ns deercreeklabs.lancaster-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.walk :as walk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.pcf :as pcf]
   [deercreeklabs.lancaster.resolution :as reso]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:clj
     (:import
      (org.apache.avro Schema
                       SchemaNormalization
                       Schema$Parser))))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

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

(defn xf-byte-arrays
  [edn-schema]
  (walk/postwalk #(if (ba/byte-array? %)
                    (u/byte-array->byte-str %)
                    %)
                 edn-schema))

#?(:clj
   (defn fp-matches? [schema]
     (let [json-schema (l/get-json-schema schema)
           parser (Schema$Parser.)
           java-schema (.parse parser ^String json-schema)
           java-fp (SchemaNormalization/parsingFingerprint64 java-schema)
           clj-fp (l/get-fingerprint64 schema)]
       (or (= java-fp clj-fp)
           (let [java-pcf (SchemaNormalization/toParsingForm java-schema)
                 clj-pcf (l/get-parsing-canonical-form schema)
                 err-str (str "Fingerprints do not match!\n"
                              "java-fp: " java-fp "\n"
                              "clj-fp: " clj-fp "\n"
                              "java-pcf:\n" java-pcf "\n"
                              "clj-pcf:\n" clj-pcf "\n")]
             (errorf err-str))))))

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

(l/def-record-schema rec-w-maybe-field-schema
  [:name l/string-schema]
  [:age (l/maybe l/int-schema)])

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested l/int-schema]
  [:qty-added l/int-schema]
  [:current-qty l/int-schema]
  [:req add-to-cart-req-schema {:sku 10 :qty-requested 1}]
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

(def sku->qty-schema (l/make-flex-map-schema l/int-schema l/int-schema))

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

(def merged-date-time-schema
  (l/merge-record-schemas ::date-time [date-schema time-schema]))

(l/def-record-schema tree-schema
  [:value l/int-schema]
  [:right (l/maybe :tree)]
  [:left (l/maybe :tree)])

(deftest test-record-schema
  (let [expected-pcf (str "{\"name\":\"deercreeklabs.lancaster_test."
                          "AddToCartReq\",\"type\":\"record\",\"fields\":"
                          "[{\"name\":\"sku\",\"type\":\"int\"},{\"name\":"
                          "\"qtyRequested\",\"type\":\"int\"}]}")
        expected-edn-schema {:name :deercreeklabs.lancaster-test/add-to-cart-req
                             :type :record
                             :fields
                             [{:name :sku
                               :type :int
                               :default -1}
                              {:name :qty-requested
                               :type :int
                               :default 0}]}]
    #?(:clj (is (fp-matches? add-to-cart-req-schema)))
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
  (is (= {:name :deercreeklabs.lancaster-test/why
          :type :enum
          :symbols [:all :stock :limit]}
         (l/get-edn-schema why-schema)))
  #?(:clj (is (fp-matches? why-schema)))
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
  (is (= {:name :deercreeklabs.lancaster-test/a-fixed
          :type :fixed
          :size 2}
         (l/get-edn-schema a-fixed-schema)))
  #?(:clj (is (fp-matches? a-fixed-schema)))
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

(deftest test-nested-record-schema
  (let [expected {:name :deercreeklabs.lancaster-test/add-to-cart-rsp
                  :type :record
                  :fields
                  [{:name :qty-requested :type :int :default -1}
                   {:name :qty-added :type :int :default -1}
                   {:name :current-qty :type :int :default -1}
                   {:name :req
                    :type
                    {:name :deercreeklabs.lancaster-test/add-to-cart-req
                     :type :record
                     :fields
                     [{:name :sku :type :int :default -1}
                      {:name :qty-requested :type :int :default 0}]}
                    :default {:sku 10 :qty-requested 1}}
                   {:name :the-reason-why
                    :type
                    {:name :deercreeklabs.lancaster-test/why
                     :type :enum
                     :symbols [:all :stock :limit]}
                    :default :stock}
                   {:name :data
                    :type
                    {:name :deercreeklabs.lancaster-test/a-fixed
                     :type :fixed
                     :size 2}
                    :default "MX"}
                   {:name :other-data
                    :type :bytes
                    :default ""}]}]
    #?(:clj (is (fp-matches? add-to-cart-rsp-schema)))
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
              :qty-added 10
              :current-qty 10
              :req {:sku 123 :qty-requested 123}
              :the-reason-why :limit
              :data (ba/byte-array [66 67])
              :other-data (ba/byte-array [123 123])}
        encoded (l/serialize add-to-cart-rsp-schema data)
        decoded (deserialize-same add-to-cart-rsp-schema
                                  encoded)]
    (is (= "9gEUFPYB9gEEQkMEe3s=" (ba/byte-array->b64 encoded)))
    (is (= (xf-byte-arrays data)
           (xf-byte-arrays decoded)))))

(deftest test-null-schema
  (let [data nil
        encoded (l/serialize l/null-schema data)
        decoded (deserialize-same l/null-schema encoded)]
    #?(:clj (is (fp-matches? l/null-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array []) encoded))
    (is (= data decoded))))

(deftest test-boolean-schema
  (let [data true
        encoded (l/serialize l/boolean-schema data)
        decoded (deserialize-same l/boolean-schema encoded)]
    #?(:clj (is (fp-matches? l/boolean-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [1]) encoded))
    (is (= data decoded))))

(deftest test-int-schema-serdes
  (let [data 7890
        encoded (l/serialize l/int-schema data)
        decoded (deserialize-same l/int-schema encoded)]
    #?(:clj (is (fp-matches? l/int-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-92 123]) encoded))
    (is (= data decoded))))

(deftest test-long-schema-serdes
  (let [data (u/ints->long 2147483647 -1)
        encoded (l/serialize l/long-schema data)
        decoded (deserialize-same l/long-schema encoded)]
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
        decoded (deserialize-same l/float-schema encoded)
        abs-err (get-abs-err data decoded)]
    #?(:clj (is (fp-matches? l/float-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-48 15 73 64]) encoded))
    (is (< abs-err 0.000001))))

(deftest test-double-schema
  (let [data (double 3.14159265359)
        encoded (l/serialize l/double-schema data)
        decoded (deserialize-same l/double-schema encoded)
        abs-err (get-abs-err data decoded)]
    #?(:clj (is (fp-matches? l/double-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [-22 46 68 84 -5 33 9 64])
                                    encoded))
    (is (< abs-err 0.000001))))

(deftest test-bytes-schema
  (let [data (ba/byte-array [1 1 2 3 5 8 13 21])
        encoded (l/serialize l/bytes-schema data)
        decoded (deserialize-same l/bytes-schema encoded)]
    #?(:clj (is (fp-matches? l/bytes-schema)))
    (is (ba/equivalent-byte-arrays? (ba/byte-array [16 1 1 2 3 5 8 13 21])
                                    encoded))
    (is (ba/equivalent-byte-arrays? data decoded))))

(deftest test-string-schema
  (let [data "Hello world!"
        encoded (l/serialize l/string-schema data)
        decoded (deserialize-same l/string-schema encoded)]
    #?(:clj (is (fp-matches? l/string-schema)))
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [24 72 101 108 108 111 32 119 111 114 108 100 33])
         encoded))
    (is (= data decoded))))

(deftest test-map-schema
  (is (= {:type :map :values :int}
         (l/get-edn-schema ages-schema)))
  #?(:clj (is (fp-matches? ages-schema)))
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

(deftest test-flex-map-schema
  (is (= {:name
          :flex-map-record-8247732601305521295-8247732601305521295,
          :type :record,
          :fields
          [{:name :ks, :type {:type :array, :items :int}, :default []}
           {:name :vs,
            :type {:type :array, :items :int},
            :default []}]}
         (l/get-edn-schema sku->qty-schema)))
  #?(:clj (is (fp-matches? sku->qty-schema)))
  (is (= (str
          "{\"name\":\"FlexMapRecord82477326013055212958247732601305521295\","
          "\"type\":\"record\",\"fields\":[{\"name\":\"ks\",\"type\":{\"type\""
          ":\"array\",\"items\":\"int\"}},{\"name\":\"vs\",\"type\":{\"type\":"
          "\"array\",\"items\":\"int\"}}]}")
         (l/get-parsing-canonical-form sku->qty-schema)))
  (is (= "-1342815058390831865"
         (u/long->str (l/get-fingerprint64 sku->qty-schema)))))

(deftest test-flex-schema-serdes
  (let [data {123 10
              456 100
              789 2}
        encoded (l/serialize sku->qty-schema data)
        decoded (deserialize-same sku->qty-schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [6, -10, 1, -112, 7, -86, 12, 0, 6, 20, -56, 1, 4, 0])
         encoded))
    (is (= data decoded))))

(deftest test-complex-key-flex-schema-serdes
  (let [schema (l/make-flex-map-schema add-to-cart-req-schema l/boolean-schema)
        data {{:sku 123 :qty-requested 10} true
              {:sku 999 :qty-requested 7} false}
        encoded (l/serialize schema data)
        decoded (deserialize-same schema encoded)]
    (is (ba/equivalent-byte-arrays?
         (ba/byte-array [4, -10, 1, 20, -50, 15, 14, 0, 4, 1, 0, 0])
         encoded))
    (is (= data decoded))))

(deftest test-plumatic-flex-map
  (let [child-schema (l/make-flex-map-schema add-to-cart-req-schema
                                             l/boolean-schema)
        schema (l/make-array-schema child-schema)
        pschema (l/get-plumatic-schema schema)
        data [{{:sku 123 :qty-requested 10} true
               {:sku 999 :qty-requested 7} false}]]
    (is (nil? (s/check pschema data)))))

(deftest test-array-schema
  #?(:clj (is (fp-matches? simple-array-schema)))
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

(deftest test-empty-array-serdes
  (let [sch (l/make-array-schema l/int-schema)
        pcf (l/get-parsing-canonical-form sch)
        data []
        encoded (l/serialize sch data)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0])
               encoded))
        decoded (l/deserialize sch pcf encoded)]
    (is (= data decoded))))

(deftest test-nested-array-schema
  #?(:clj (is (fp-matches? rsps-schema)))
  (is (= {:type :array
          :items
          {:name :deercreeklabs.lancaster-test/add-to-cart-rsp
           :type :record
           :fields
           [{:name :qty-requested :type :int :default -1}
            {:name :qty-added :type :int :default -1}
            {:name :current-qty :type :int :default -1}
            {:name :req
             :type
             {:name :deercreeklabs.lancaster-test/add-to-cart-req
              :type :record
              :fields
              [{:name :sku :type :int :default -1}
               {:name :qty-requested :type :int :default 0}]}
             :default {:sku 10 :qty-requested 1}}
            {:name :the-reason-why
             :type
             {:name :deercreeklabs.lancaster-test/why
              :type :enum
              :symbols [:all :stock :limit]}
             :default :stock}
            {:name :data
             :type
             {:name :deercreeklabs.lancaster-test/a-fixed
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
  #?(:clj (is (fp-matches? nested-map-schema)))
  (is (= {:type :map
          :values
          {:name :deercreeklabs.lancaster-test/add-to-cart-rsp
           :type :record
           :fields
           [{:name :qty-requested :type :int :default -1}
            {:name :qty-added :type :int :default -1}
            {:name :current-qty :type :int :default -1}
            {:name :req
             :type
             {:name :deercreeklabs.lancaster-test/add-to-cart-req
              :type :record
              :fields
              [{:name :sku :type :int :default -1}
               {:name :qty-requested :type :int :default 0}]}
             :default {:sku 10 :qty-requested 1}}
            {:name :the-reason-why
             :type
             {:name :deercreeklabs.lancaster-test/why
              :type :enum
              :symbols [:all :stock :limit]}
             :default :stock}
            {:name :data
             :type
             {:name :deercreeklabs.lancaster-test/a-fixed
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

(deftest test-empty-map-serdes
  (let [sch (l/make-map-schema l/int-schema)
        pcf (l/get-parsing-canonical-form sch)
        data {}
        encoded (l/serialize sch data)
        _ (is (ba/equivalent-byte-arrays?
               (ba/byte-array [0])
               encoded))
        decoded (l/deserialize sch pcf encoded)]
    (is (= data decoded))))

(deftest test-union-schema
  #?(:clj (is (fp-matches? union-schema)))
  (is (= [:int
          {:name :deercreeklabs.lancaster-test/add-to-cart-req
           :type :record
           :fields
           [{:name :sku :type :int :default -1}
            {:name :qty-requested :type :int :default 0}]}
          {:name :deercreeklabs.lancaster-test/a-fixed
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
  #?(:clj (is (fp-matches? person-or-dog-schema)))
  (is (= [{:name :deercreeklabs.lancaster-test/person
           :type :record
           :fields
           [{:name :name :type :string :default "No name"}
            {:name :age :type :int :default 0}]}
          {:name :deercreeklabs.lancaster-test/dog
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
  #?(:clj (is (fp-matches? map-or-array-schema)))
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
  #?(:clj (is (fp-matches? mopodoa-schema)))
  (is (= [{:type :map :values :int}
          {:name :deercreeklabs.lancaster-test/person
           :type :record
           :fields [{:name :name :type :string :default "No name"}
                    {:name :age :type :int :default 0}]}
          {:name :deercreeklabs.lancaster-test/dog
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

(deftest test-recursive-schema
  (is (= {:name :deercreeklabs.lancaster-test/tree
          :type :record
          :fields
          [{:name :value :type :int :default -1}
           {:name :right
            :type [:null :tree]
            :default nil}
           {:name :left
            :type [:null :tree]
            :default nil}]}
         (l/get-edn-schema tree-schema)))
  #?(:clj (is (fp-matches? tree-schema)))
  (is (= "1955448859740230833"
         (u/long->str (l/get-fingerprint64 tree-schema)))))

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

(deftest  test-schema-resolution-enum-added-symbol
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
  (let [data {:name "Runner" :owner "Tommy" :tag-number 134}
        wrapped-data (l/wrap dog-schema data)
        writer-schema fish-or-person-or-dog-v2-schema
        reader-schema person-or-dog-schema
        encoded (l/serialize writer-schema wrapped-data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        [schema-name decoded-data] decoded
        expected (dissoc data :tag-number)]
    (is (= expected decoded-data))))

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

(deftest test-schema-evolution-named-ref
  (let [data {:players [{:first "Chad" :last "Harrington"}]
              :judges [{:first "Chibuzor" :last "Okonkwo"}]}
        name-schema (l/make-record-schema
                     ::name
                     [[:first l/string-schema]
                      [:last l/string-schema]])
        writer-schema (l/make-record-schema
                       ::game
                       [[:players (l/make-array-schema name-schema)]
                        [:judges (l/make-array-schema name-schema)]])
        reader-schema (l/make-record-schema
                       ::game
                       [[:players (l/make-array-schema name-schema)]
                        [:judges (l/make-array-schema name-schema)]
                        [:audience (l/make-array-schema name-schema)]])
        encoded (l/serialize writer-schema data)
        writer-pcf (l/get-parsing-canonical-form writer-schema)
        decoded (l/deserialize reader-schema writer-pcf encoded)
        expected (assoc data :audience [])]
    (is (= (str "{\"name\":\"deercreeklabs.lancaster_test.Game\",\"type\":"
                "\"record\",\"fields\":[{\"name\":\"players\",\"type\":"
                "{\"type\":\"array\",\"items\":{\"name\":"
                "\"deercreeklabs.lancaster_test.Name\",\"type\":\"record\","
                "\"fields\":[{\"name\":\"first\",\"type\":\"string\"},"
                "{\"name\":\"last\",\"type\":\"string\"}]}}},{\"name\":"
                "\"judges\",\"type\":{\"type\":\"array\",\"items\":"
                "\"deercreeklabs.lancaster_test.Name\"}}]}")
           writer-pcf))
    (is (= expected decoded))))

(deftest test-rec-w-array-and-enum-schema
  #?(:clj (is (fp-matches? rec-w-array-and-enum-schema)))
  (is (= {:name :deercreeklabs.lancaster-test/rec-w-array-and-enum
          :type :record
          :fields
          [{:name :names
            :type {:type :array :items :string}
            :default []}
           {:name :why
            :type
            {:name :deercreeklabs.lancaster-test/why
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
  #?(:clj (is (fp-matches? rec-w-map-schema)))
  (is (= {:name :deercreeklabs.lancaster-test/rec-w-map
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
  #?(:clj (is (fp-matches? rec-w-fixed-no-default-schema)))
  (is (= {:name :deercreeklabs.lancaster-test/rec-w-fixed-no-default
          :type :record
          :fields
          [{:name :data
            :type
            {:name :deercreeklabs.lancaster-test/a-fixed
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

(deftest test-rec-w-maybe-field
  #?(:clj (is (fp-matches? rec-w-maybe-field-schema)))
  (is (= {:name :deercreeklabs.lancaster-test/rec-w-maybe-field,
          :type :record,
          :fields
          [{:name :name, :type :string, :default ""}
           {:name :age, :type [:null :int], :default nil}]}
         (l/get-edn-schema rec-w-maybe-field-schema)))
  (is (= "7746454544656991807"
         (u/long->str (l/get-fingerprint64 rec-w-maybe-field-schema)))))

(deftest test-record-serdes-missing-field
  (let [data {:sku 100}]
    (try
      (l/serialize add-to-cart-req-schema data)
      (is (= :did-not-throw :but-should-have))
      (catch #?(:cljs js/Error :clj Exception) e
        (is (str/includes?
             (lu/get-exception-msg e)
             (str "is not a valid :int. Path: [:qty-requested]")))))))

(deftest test-record-serdes-missing-maybe-field
  (let [data {:name "Sharon"}
        encoded (l/serialize rec-w-maybe-field-schema data)
        decoded (deserialize-same rec-w-maybe-field-schema encoded)]
    (is (= (assoc data :age nil) decoded))))

(deftest test-plumatic-primitives
  (is (= u/Nil (l/get-plumatic-schema l/null-schema)))
  (is (= s/Bool (l/get-plumatic-schema l/boolean-schema)))
  (is (= s/Int (l/get-plumatic-schema l/int-schema)))
  (is (= u/LongOrInt (l/get-plumatic-schema l/long-schema)))
  (is (= s/Num (l/get-plumatic-schema l/float-schema)))
  (is (= s/Num (l/get-plumatic-schema l/double-schema)))
  (is (= u/StringOrBytes (l/get-plumatic-schema l/string-schema)))
  (is (= u/StringOrBytes (l/get-plumatic-schema l/bytes-schema))))

(deftest test-plumatic-records
  (let [expected {s/Any s/Any
                  (s/required-key :sku) s/Int
                  (s/required-key :qty-requested) s/Int}
        _ (is (= expected (l/get-plumatic-schema add-to-cart-req-schema)))
        expected {s/Any s/Any
                  :names [u/StringOrBytes]
                  :why (s/enum :all :stock :limit)}
        _ (is (= expected
                 (l/get-plumatic-schema rec-w-array-and-enum-schema)))]))

(deftest test-plumatic-union-unwrapped
  (let [expected (s/conditional
                  int? s/Int
                  map? (l/get-plumatic-schema add-to-cart-req-schema)
                  u/valid-bytes-or-string? u/StringOrBytes)]
    (is (= expected (l/get-plumatic-schema union-schema)))))

(deftest test-plumatic-union-wrapped
  (let [pl-sch (l/get-plumatic-schema person-or-dog-schema)
        wrapped-data (l/wrap person-schema {:name "Apollo"
                                            :age 30})
        bad-wrapped-data (l/wrap person-schema {:name "Apollo"
                                                :age "thirty"})]
    (is (= nil (s/check pl-sch wrapped-data)))
    (is (not= nil (s/check pl-sch bad-wrapped-data)))))

(deftest test-merge-record-schemas
  #?(:clj (is (fp-matches? merged-date-time-schema)))
  (let [expected {:name :deercreeklabs.lancaster-test/date-time
                  :type :record
                  :fields
                  [{:name :year :type :int :default -1}
                   {:name :month :type :int :default -1}
                   {:name :day :type :int :default -1}
                   {:name :hour :type :int :default -1}
                   {:name :minute :type :int :default -1}
                   {:name :second :type [:null :int] :default nil}]}
        merged-name (:name
                     (l/get-edn-schema merged-date-time-schema))
        normal-name (:name
                     (l/get-edn-schema date-time-schema))]
    (is (= merged-name normal-name))
    (is (= expected (l/get-edn-schema merged-date-time-schema)))))

(deftest test-plumatic-maybe-missing-key
  (let [ps (l/get-plumatic-schema rec-w-maybe-field-schema)
        data {:name "Boomer"}]
    (is (= nil (s/check ps data)))))

(deftest test-plumatic-maybe-nil-value
  (let [ps (l/get-plumatic-schema rec-w-maybe-field-schema)
        data {:name "Boomer"
              :age nil}]
    (is (= nil (s/check ps data)))))

(deftest test-forgot-wrapping
  (let [data {:name "Cally"
              :age 24}]
    (try
      (l/serialize person-or-dog-schema data)
      (is (= :should-have-thrown :but-didnt))
      (catch #?(:clj Exception :cljs js/Error) e
        (is (str/includes? (lu/get-exception-msg e)
                           "Union requires wrapping"))))))

(deftest test-schema?
  (is (l/schema? person-or-dog-schema))
  (is (not (l/schema? :foo))))

(deftest test-deserialize-from-pcf
  (let [pcf (l/get-parsing-canonical-form add-to-cart-rsp-schema)
        data {:qty-requested 123
              :qty-added 10
              :current-qty 10
              :req {:sku 123 :qty-requested 123}
              :the-reason-why :limit
              :data (ba/byte-array [66 67])
              :other-data (ba/byte-array [123 123])}
        encoded (l/serialize add-to-cart-rsp-schema data)
        sch (->> (pcf/pcf->edn-schema pcf)
                 (schemas/edn-schema->lancaster-schema :record))
        decoded (l/deserialize sch pcf encoded)]
    (is (= (dissoc data :data :other-data)
           (dissoc decoded :data :other-data)))))

(deftest test-bad-serialize-arg
  (s/without-fn-validation ;; Allow built-in handlers to throw
   (try
     (l/serialize nil nil)
     (is (= :should-have-thrown :but-didnt))
     (catch #?(:clj Exception :cljs js/Error) e
       (let [msg (lu/get-exception-msg e)]
         (is (str/includes?
              msg "First argument to serialize must be a schema object")))))
   (try
     (l/deserialize why-schema nil (ba/byte-array []))
     (is (= :should-have-thrown :but-didnt))
     (catch #?(:clj Exception :cljs js/Error) e
       (let [msg (lu/get-exception-msg e)]
         (is (str/includes?
              msg "parsing canonical form")))))
   (try
     (l/deserialize why-schema (l/get-parsing-canonical-form why-schema) [])
     (is (= :should-have-thrown :but-didnt))
     (catch #?(:clj Exception :cljs js/Error) e
       (let [msg (lu/get-exception-msg e)]
         (is (str/includes?
              msg "Third argument to deserialize must be a byte array")))))))

(deftest test-bad-deserialize-args
  (s/without-fn-validation ;; Allow built-in handlers to throw
   (try
     (l/deserialize nil (l/get-parsing-canonical-form why-schema)
                    (ba/byte-array []))
     (is (= :should-have-thrown :but-didnt))
     (catch #?(:clj Exception :cljs js/Error) e
       (let [msg (lu/get-exception-msg e)]
         (is (str/includes?
              msg "First argument to deserialize must be a schema object")))))
   (try
     (l/deserialize why-schema nil (ba/byte-array []))
     (is (= :should-have-thrown :but-didnt))
     (catch #?(:clj Exception :cljs js/Error) e
       (let [msg (lu/get-exception-msg e)]
         (is (str/includes?
              msg "parsing canonical form")))))
   (try
     (l/deserialize why-schema (l/get-parsing-canonical-form why-schema) [])
     (is (= :should-have-thrown :but-didnt))
     (catch #?(:clj Exception :cljs js/Error) e
       (let [msg (lu/get-exception-msg e)]
         (is (str/includes?
              msg "Third argument to deserialize must be a byte array")))))))

(deftest test-field-default-validation
  (try
    (l/make-record-schema :test-schema
                          [[:int-field l/int-schema "a"]])
    (is (= :should-have-thrown :but-didnt))
    (catch #?(:clj Exception :cljs js/Error) e
      (let [msg (lu/get-exception-msg e)]
        (is (re-find #"Default value for field .* is invalid" msg))))))

(deftest test-make-default-data
  (is (= :all (l/make-default-data why-schema)))
  (is (= {:sku -1, :qty-requested 0}
         (l/make-default-data add-to-cart-req-schema))))

(deftest test-bad-field-name
  (try
    (l/make-record-schema :test-schema
                          [[:bad? l/boolean-schema]])
    (is (= :should-have-thrown :but-didnt))
    (catch #?(:clj Exception :cljs js/Error) e
      (let [msg (lu/get-exception-msg e)]
        (is (re-find #"Name keywords must start with a letter and subsequently"
                     msg))))))

(deftest test-bad-record-name
  (try
    (l/make-record-schema :*test-schema*
                          [[:is-good l/boolean-schema]])
    (is (= :should-have-thrown :but-didnt))
    (catch #?(:clj Exception :cljs js/Error) e
      (let [msg (lu/get-exception-msg e)]
        (is (re-find #"Name keywords must start with a letter and subsequently"
                     msg))))))
