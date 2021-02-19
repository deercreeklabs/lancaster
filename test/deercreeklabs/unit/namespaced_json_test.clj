(ns deercreeklabs.unit.namespaced-json-test
  (:require
   [clojure.test :refer [deftest is]]
   [deercreeklabs.lancaster :as l]))

(deftest test-namespaced-enums-from-json
  (let [schema (l/json->schema (slurp "test/namespaced_enums.json"))
        obj {:foo :foo-x :bar :foo-y}]
    (is (= :test-namespaced-enums (get-in schema [:edn-schema :name])))
    (is (= [2 0 2 2]
           (map int (l/serialize schema obj))))
    (is (= obj (l/deserialize-same schema (l/serialize schema obj))))))

(deftest test-namespaced-records-from-json
  (let [schema (l/json->schema (slurp "test/namespaced_records.json"))
        obj {:foo {:baz "a string"} :bar {}}]
    (is (= :test-namespaced-records (get-in schema [:edn-schema :name])))
    (is (= [2, 2, 16, 97, 32, 115, 116, 114, 105, 110, 103, 2, 0]
           (map int (l/serialize schema obj))))
    (is (= obj (l/deserialize-same schema (l/serialize schema obj))))))
