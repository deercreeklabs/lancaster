(ns deercreeklabs.unit.namespaced-json-test
  (:require
   [clojure.data :as data]
   [clojure.set :as set]
   [clojure.test :refer [deftest is]]
   [cheshire.core :as json]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u])
  (:import org.apache.avro.Schema$Parser
           org.apache.avro.SchemaNormalization))
(comment
 (let [schema (l/json->schema (slurp "test/namespaced_enums.json"))
       obj {:foo :foo-x :bar :foo-y}]
   (println (map int (l/serialize schema obj)))
   #_(= :com.company/test-namespaced-enums (get-in schema [:edn-schema :name]))
   #_(= [2 0 2 2]
      (map int (l/serialize schema obj)))
   (= obj (l/deserialize-same schema (l/serialize schema obj)))))

(deftest test-namespaced-enums-from-json
  (let [schema (l/json->schema (slurp "test/namespaced_enums.json"))
        obj {:foo :foo-x :bar :foo-y}]
    (is (= :com.company/test-namespaced-enums (get-in schema [:edn-schema :name])))
    (is (= [2 0 2 2]
           (map int (l/serialize schema obj))))
    (is (= obj (l/deserialize-same schema (l/serialize schema obj))))))

(comment
 (let [schema (l/json->schema (slurp "test/namespaced_records.json"))
       obj {:foo {:baz "a string"
                  :subfoo {:x 1}
                  :subfoofoo {:x 2}} :bar {}}]
   (println (get-in schema [:edn-schema :name]))
   (println (= :com.company/test-namespaced-records (get-in schema [:edn-schema :name])))
   #_(is (= '(2 2 16 97 32 115 116 114 105 110 103 2 2 2 2 2 4 2 0 0 0)
            (map int (l/serialize schema obj))))
   #_(is (= obj (l/deserialize-same schema (l/serialize schema obj))))))

(deftest test-namespaced-records-from-json
  (let [schema (l/json->schema (slurp "test/namespaced_records.json"))
        obj {:foo {:baz "a string"
                   :subfoo {:x 1}
                   :subfoofoo {:x 2}} :bar {}}]
    (is (= :com.company/test-namespaced-records (get-in schema [:edn-schema :name])))
    (is (set/subset? #{:long
                       :double
                       :com.company/test-namespaced-records
                       :int
                       :sub-foo-record
                       :float
                       :com.company.foo-bar/foo-record
                       :string
                       :null
                       :test-namespaced-records
                       :com.company.foo-bar/sub-foo-record
                       :bytes
                       :foo-record
                       :boolean}
                     (set (keys @u/*__INTERNAL__name->schema))))
    (is (set/subset? #{:long
                       :double
                       :com.company/test-namespaced-records
                       :int
                       :sub-foo-record
                       :float
                       :com.company.foo-bar/foo-record
                       :string
                       :null
                       :test-namespaced-records
                       :com.company.foo-bar/sub-foo-record
                       :bytes
                       :foo-record
                       :boolean}
                     (set (keys @u/*__INTERNAL__name->schema))))
    (is (= '(2 2 16 97 32 115 116 114 105 110 103 2 2 2 2 2 4 2 0 0 0)
           (map int (l/serialize schema obj))))
    (is (= obj (l/deserialize-same schema (l/serialize schema obj))))))

(deftest test-namespaced-schema-pcf
  (is (= (SchemaNormalization/toParsingForm
          (.parse (Schema$Parser.)
                  (slurp "test/namespaced_records.json")))
         (l/pcf
          (l/json->schema
           (slurp "test/namespaced_records.json"))))))
