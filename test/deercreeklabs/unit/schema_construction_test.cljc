(ns deercreeklabs.unit.schema-construction-test
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as ltest]
   [schema.core :as s])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(l/def-enum-schema tax-type-schema
  "Whether or not this is taxable"
  :taxable :exempt)

(def coins-schema
  (l/enum-schema ::coins
                 "Common US coins"
                 [:penny :nickel :dime :quarter]))

;; [field-name [docstring] [:required] schema [default]]

(l/def-record-schema invoice-schema
  "A schema representing invoices"
  [:invoice/id "The unique id" :required l/string-schema]
  [:customer/id :required l/string-schema]
  [:date-ms "The date in UTC" l/long-schema]
  [:tax-type :required tax-type-schema :taxable]
  [:item-ids (l/array-schema l/string-schema)])

(def invoice2-schema
  (l/record-schema ::invoice-schema
                   "This schema doesn't have tax-type"
                   [[:invoice/id "The unique id" :required l/string-schema]
                    [:customer/id :required l/string-schema]
                    [:date-ms l/long-schema]
                    [:item-ids (l/array-schema l/string-schema)]]))

(deftest test-enum-schema-w-docstring
  (is (= {:default :taxable,
          :doc "Whether or not this is taxable",
          :name :deercreeklabs.unit.schema-construction-test/tax-type,
          :symbols [:taxable :exempt],
          :type :enum}
         (l/edn tax-type-schema)))
  (is (= (str "{\"name\":\"deercreeklabs.unit.schema_construction_test.TaxType"
              "\",\"type\":\"enum\",\"symbols\":[\"TAXABLE\",\"EXEMPT\"],"
              "\"default\":\"TAXABLE\",\"doc\":\"Whether or not this is "
              "taxable\"}")
         (l/json tax-type-schema)))
  (let [data :exempt
        encoded (l/serialize tax-type-schema data)
        decoded (l/deserialize-same tax-type-schema encoded)]
    (is (= data decoded))))

(deftest test-record-schema-options
  (is (= {:type :record
          :name :deercreeklabs.unit.schema-construction-test/invoice
          :doc "A schema representing invoices"
          :fields [{:name :invoice/id
                    :doc "The unique id"
                    :type :string
                    :default ""}
                   {:name :customer/id
                    :type :string
                    :default ""}
                   {:name :date-ms
                    :doc "The date in UTC"
                    :type [:null :long]
                    :default nil}
                   {:name :tax-type
                    :type {:default :taxable,
                           :doc "Whether or not this is taxable",
                           :name :deercreeklabs.unit.schema-construction-test/tax-type,
                           :symbols [:taxable :exempt],
                           :type :enum}
                    :default :taxable}
                   {:name :item-ids
                    :type [:null {:items :string
                                  :type :array}]
                    :default nil}]}
         (l/edn invoice-schema)))
  (is (= (str
          "{\"name\":\"deercreeklabs.unit.schema_construction_test.Invoice\","
          "\"type\":\"record\",\"fields\":[{\"name\":\"invoiceId\",\"type\":"
          "\"string\",\"default\":\"\",\"doc\":\"The unique id\"},{\"name\":"
          "\"customerId\",\"type\":\"string\",\"default\":\"\"},{\"name\":"
          "\"dateMs\",\"type\":[\"null\",\"long\"],\"default\":null,\"doc\":"
          "\"The date in UTC\"},{\"name\":\"taxType\",\"type\":{\"name\":"
          "\"deercreeklabs.unit.schema_construction_test.TaxType\",\"type\":"
          "\"enum\",\"symbols\":[\"TAXABLE\",\"EXEMPT\"],\"default\":"
          "\"TAXABLE\",\"doc\":\"Whether or not this is taxable\"},\"default\":"
          "\"TAXABLE\"},{\"name\":\"itemIds\",\"type\":[\"null\",{\"type\":"
          "\"array\",\"items\":\"string\"}],\"default\":null}],\"doc\":"
          "\"A schema representing invoices\"}")
         (l/json invoice-schema)))
  (is (= (str
          "{\"name\":\"deercreeklabs.unit.schema_construction_test.Invoice\","
          "\"type\":\"record\",\"fields\":[{\"name\":\"invoiceId\",\"type\":"
          "\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":"
          "\"dateMs\",\"type\":[\"null\",\"long\"]},{\"name\":\"taxType\","
          "\"type\":{\"name\":\"deercreeklabs.unit.schema_construction_test."
          "TaxType\",\"type\":\"enum\",\"symbols\":[\"TAXABLE\",\"EXEMPT\"]}},"
          "{\"name\":\"itemIds\",\"type\":[\"null\",{\"type\":\"array\","
          "\"items\":\"string\"}]}]}")
         (l/pcf invoice-schema)))
  (is (= "679893850691967354"
         (u/long->str (l/fingerprint64 invoice-schema))))
  (is (ltest/fp-matches? invoice-schema)))

(deftest test-round-trip-invoice
  (let [data {:invoice/id "id001"
              :customer/id "cust007"
              ;; No date provided
              :tax-type :taxable
              :item-ids ["item43" "item689"]}
        expected (assoc data :date-ms nil)
        encoded (l/serialize invoice-schema data)
        decoded (l/deserialize-same invoice-schema encoded)]
    (is (= expected decoded))))
