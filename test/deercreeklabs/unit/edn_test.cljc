(ns deercreeklabs.unit.edn-test
  (:require
   [clojure.test :refer [are deftest is]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.unit.lancaster-test :as lt]))

(l/def-enum-schema suits-schema
  :hearts :diamonds :spades :clubs)

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(l/def-array-schema users-schema
  user-schema)

(l/def-record-schema msg-schema
  [:user user-schema]
  [:text l/string-schema])

(deftest test-edn->schema
  (are [edn] (= edn (-> edn l/edn->schema l/edn))
    :string
    :int
    [:null :int :string]
    {:name ::test-fixed
     :type :fixed
     :size 16}
    {:name ::test-enum
     :type :enum
     :symbols [:a :b :c]}
    {:name ::test-rec
     :type :record
     :fields [{:name :a
               :type :int
               :default -1}
              {:name :b
               :type :string
               :default ""}]}) )

(deftest test-name-kw
  (let [cases [[l/string-schema :string]
               [l/int-schema :int]
               [lt/why-schema :deercreeklabs.unit.lancaster-test/why]
               [lt/a-fixed-schema :deercreeklabs.unit.lancaster-test/a-fixed]
               [lt/ages-schema :map]
               [lt/simple-array-schema :array]
               [lt/rec-w-array-and-enum-schema
                :deercreeklabs.unit.lancaster-test/rec-w-array-and-enum]
               [lt/maybe-int-schema :union]]]
    (doseq [[sch expected] cases]
      (let [edn (l/edn sch)
            ret (l/name-kw sch)]
        (is (= [edn expected] [edn ret]))))))

(deftest test-round-trip-json-schema
  (let [json-schema (str "{\"type\":\"record\",\"name\":"
                         "\"StringMinimalExample\",\"namespace\":"
                         "\"com.piotr-yuxuan\",\"fields\":[{\"name\":"
                         "\"someOtherField\",\"type\":[\"null\",\"long\"]},"
                         "{\"name\":\"url\",\"type\":{\"type\":\"string\","
                         "\"avro.java.string\":\"String\"}}]}")
        schema (l/json->schema json-schema)
        rt-json-schema (l/json schema)]
    (is (= json-schema rt-json-schema))))

(deftest test-json-schema-w-evolution-no-default
  (let [data {:a 1}
        writer-schema (l/record-schema ::test-rec [[:a l/int-schema]])
        reader-json (str
                     "{\"name\":\"deercreeklabs.unit.edn_test.TestRec\","
                     "\"type\":\"record\",\"fields\":["
                     "{\"name\":\"a\",\"type\":\"int\"},"
                     "{\"name\":\"b\",\"type\":\"string\"}]}")
        reader-schema (l/json->schema reader-json)
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)
        expected (assoc data :b "")]
    (is (= expected decoded))))

#?(:clj
   (deftest test-issue-4
     (let [json (slurp "test/example.avsc")
           schema (l/json->schema json)]
       (is (= json (l/json schema))))))

(deftest test-enum-pcf-rt
  (let [orig-edn (l/edn suits-schema)
        rt-edn (-> (l/pcf suits-schema)
                   (l/json->schema)
                   (l/edn))
        expected {:default :hearts
                  :name :deercreeklabs.unit.edn-test/suits
                  :namespace :deercreeklabs.unit.edn-test
                  :symbols [:hearts :diamonds :spades :clubs]
                  :type :enum}]
    (is (= expected orig-edn))
    (is (= expected rt-edn))))

(deftest test-record-pcf-rt
  (let [orig-edn (l/edn user-schema)
        rt-edn (-> (l/pcf user-schema)
                   (l/json->schema)
                   (l/edn))
        expected {:fields [{:name :name
                            :default nil
                            :type [:null :string]}
                           {:name :nickname
                            :default nil
                            :type [:null :string]}]
                  :name :deercreeklabs.unit.edn-test/user
                  :namespace :deercreeklabs.unit.edn-test
                  :type :record}]
    (is (= expected orig-edn))
    (is (= expected rt-edn))))

(deftest test-record-pcf-rt-nested-rec
  (let [orig-edn (l/edn msg-schema)
        rt-edn (-> (l/pcf msg-schema)
                   (l/json->schema)
                   (l/edn))
        expected {:fields [{:name :user
                            :default nil
                            :type [:null
                                   {:fields [{:default nil
                                              :name :name
                                              :type [:null :string]}
                                             {:default nil
                                              :name :nickname
                                              :type [:null :string]}],
                                    :name :deercreeklabs.unit.edn-test/user,
                                    :namespace :deercreeklabs.unit.edn-test,
                                    :type :record}]}
                           {:name :text
                            :default nil
                            :type [:null :string]}]
                  :name :deercreeklabs.unit.edn-test/msg
                  :namespace :deercreeklabs.unit.edn-test
                  :type :record}]
    (is (= expected orig-edn))
    (is (= expected rt-edn))))
