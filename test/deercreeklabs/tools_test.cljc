(ns deercreeklabs.tools-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.avro-tools :as at]
   [deercreeklabs.avro-tools.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [cljs.core.async.macros :as ca])))

(u/configure-logging)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Unit tests

(at/def-avro-rec add-to-cart-req-schema
  [:sku :int]
  [:qty :int 0])

(at/def-avro-enum why-schema
  :all :stock :limit)

;; TODO: Fix the default for :why field. (->why :all)
(at/def-avro-rec add-to-cart-rsp-schema
  [:qty-requested :int]
  [:qty-added :int]
  [:current-qty :int]
  [:req add-to-cart-req-schema]
  [:why why-schema])

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

(deftest test-nested-def-avro
  (is (= {:namespace "deercreeklabs.tools_test"
          :name "AddToCartRsp"
          :type :record
          :fields
          [{:name "qtyRequested" :type :int :default -1}
           {:name "qtyAdded" :type :int :default -1}
           {:name "currentQty" :type :int :default -1}
           {:name "req"
            :type
            {:namespace "deercreeklabs.tools_test"
             :name "AddToCartReq"
             :type :record
             :fields
             [{:name "sku" :type :int :default -1}
              {:name "qty" :type :int :default 0}]}
            :default {"sku" -1 "qty" 0}}
           {:name "why"
            :type
            {:namespace "deercreeklabs.tools_test"
             :name "Why"
             :type :enum
             :symbols ["ALL" "STOCK" "LIMIT"]}
            :default "ALL"}]}
         add-to-cart-rsp-schema)))
