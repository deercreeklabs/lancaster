(ns deercreeklabs.unit.namespaced-json-test
  (:require
   [clojure.test :refer [deftest is]]
   [cheshire.core :as json]
   [deercreeklabs.lancaster :as l])
  (:import
   (clojure.lang ExceptionInfo)
   (org.apache.avro Schema$Parser
                    SchemaNormalization)))

(deftest test-namespaced-enums-from-json
  (let [schema (l/json->schema (slurp "test/namespaced_enums.json"))
        obj {:foo :foo-x :bar :foo-y}]
    (is (= :test-namespaced-enums (get-in schema [:edn-schema :name])))
    (is (= [2 0 2 2]
           (map int (l/serialize schema obj))))
    (is (= obj (l/deserialize-same schema (l/serialize schema obj))))))

(deftest test-namespaced-records-from-json
  (let [schema (l/json->schema (slurp "test/namespaced_records.json"))
        obj {:foo {:baz "a string"
                   :subfoo {:x 1}
                   :subfoofoo {:x 2}} :bar {}}]
    (is (= :test-namespaced-records (get-in schema [:edn-schema :name])))
    (is (= #{:long
             :double
             :com.company/test-namespaced-records
             :int
             :float
             :com.company.foo-bar/foo-record
             :string
             :null
             :com.company.foo-bar/sub-foo-record
             :bytes
             :boolean} (set (keys (get-in schema [:name->edn-schema])))))
    (is (= #{:long
             :double
             :com.company/test-namespaced-records
             :int
             :float
             :com.company.foo-bar/foo-record
             :string
             :null
             :com.company.foo-bar/sub-foo-record
             :bytes
             :boolean} (set (keys @(get-in schema [:*name->serializer])))))
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
