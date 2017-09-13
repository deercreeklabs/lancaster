(ns deercreeklabs.tools-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [cljs.core.async.macros :as ca])))

(u/configure-logging)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Unit tests

(l/def-avro-rec add-to-cart-req-schema
  [:sku :int]
  [:qty :int 0])

(l/def-avro-enum why-schema
  :all :stock :limit)

(l/def-avro-fixed some-bytes-schema 16)

(l/def-avro-rec add-to-cart-rsp-schema
  [:qty-requested :int]
  [:qty-added :int]
  [:current-qty :int]
  [:req add-to-cart-req-schema]
  [:why why-schema]
  [:data some-bytes-schema])

(def ^:avro-schema a-non-macro-record
  (l/avro-rec :mysch
              [[:field1 :int]
               [:field2 :int]]))

(def a-union
  (l/avro-union [add-to-cart-req-schema add-to-cart-rsp-schema]))

(def array-schema (l/avro-array some-bytes-schema))

(deftest test-def-avro-rec
  (is (= {:namespace "deercreeklabs.tools_test"
          :name "AddToCartReq"
          :type :record
          :fields [{:name "sku" :type :int :default -1}
                   {:name "qty" :type :int :default 0}]}
         add-to-cart-req-schema)))

(deftest test-def-avro-enum
  (is (= {:namespace "deercreeklabs.tools_test"
          :name "Why"
          :type :enum
          :symbols ["ALL" "STOCK" "LIMIT"]}
         why-schema)))

(deftest test-def-avro-fixed
  (is (= {:namespace "deercreeklabs.tools_test"
          :name "SomeBytes"
          :type :fixed
          :size 16}
         some-bytes-schema)))

(deftest test-nested-def-avro
  (is (= {:namespace "deercreeklabs.tools_test"
          :name "AddToCartRsp"
          :type :record
          :fields
          [{:name "qtyRequested" :type :int :default -1}
           {:name "qtyAdded" :type :int :default -1}
           {:name "currentQty" :type :int :default -1}
           {:name "req"
            :type "AddToCartReq"
            :default {"sku" -1 "qty" 0}}
           {:name "why" :type "Why" :default "ALL"}
           {:name "data" :type "SomeBytes" :default ""}]}
         add-to-cart-rsp-schema)))

(deftest test-var-meta
  (let [schemas [#'add-to-cart-req-schema #'why-schema #'some-bytes-schema
                 #'add-to-cart-rsp-schema #'a-non-macro-record]]
    (doseq [schema schemas]
      (is (true? (:avro-schema (meta schema)))))))

(deftest test-get-named-schemas-in-ns
  (let [schemas (gen/get-named-schemas-in-ns
                 (find-ns 'deercreeklabs.tools-test))]
    (is (= 5 (count schemas)))
    (is (= {:namespace "deercreeklabs.tools_test"
            :name "AddToCartReq"
            :type :record
            :fields
            [{:name "sku" :type :int :default -1}
             {:name "qty" :type :int :default 0}]}
           (first schemas)))))

(deftest test-avro-union
  (is (= ["AddToCartReq" "AddToCartRsp"]
         a-union)))

(deftest test-avro-array
  (is (= {:type :array :values "SomeBytes"}
         array-schema)))

(deftest test-gen-classes
  (let [ns (find-ns 'deercreeklabs.tools-test)
        ret (gen/gen-classes ns "/Users/chad/Desktop/java")]
    (is (= true ret))))
