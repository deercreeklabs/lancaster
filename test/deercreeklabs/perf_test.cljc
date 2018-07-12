(ns deercreeklabs.perf-test
  (:require
   #?(:clj [cheshire.core :as json])
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! false)

(def add-to-cart-req-schema
  (l/make-record-schema ::add-to-cart-req
                        [[:sku l/int-schema]
                         [:qty-requested l/int-schema 0]]))

(def why-schema (l/make-enum-schema ::why
                                    [:all :stock :limit]))

(def a-fixed-schema (l/make-fixed-schema ::a-fixed 2))

(def rec-w-fixed-no-default-schema
  (l/make-record-schema ::rec-w-fixed-no-default
                        [[:data a-fixed-schema]]))

(def add-to-cart-rsp-schema
  (l/make-record-schema ::add-to-cart-rsp
                        [[:qty-requested l/int-schema]
                         [:qty-added l/int-schema]
                         [:current-qty l/int-schema]
                         [:req add-to-cart-req-schema
                          {:sku 10 :qty-requested 1}]
                         [:the-reason-why why-schema :stock]
                         [:data a-fixed-schema (ba/byte-array [77 88])]
                         [:other-data l/bytes-schema]]))

(defn get-ops-per-sec [f iters]
  (let [start-ms (u/get-current-time-ms)
        _ (dotimes [_ iters]
            (f))
        ms (- (u/get-current-time-ms) start-ms)]
    (/ (* 1000 iters) ms)))

(defn deserialize-same
  "Deserialize with the same reader and writer schemas. Use for testing only."
  [schema encoded]
  (l/deserialize schema (l/get-parsing-canonical-form schema) encoded))

(deftest ^:perf test-serdes-speed
  (let [data {:qty-requested 123
              :qty-added 10
              :current-qty 10
              :req {:sku 123 :qty-requested 123}
              :the-reason-why :limit
              :data (ba/byte-array [66 67])
              :other-data (ba/byte-array [123 123])}
        num-ops #?(:cljs 1e4 :clj 1e5)
        enc-fn #(l/serialize add-to-cart-rsp-schema data)
        json-enc-fn (fn []
                      #?(:clj (json/generate-string data)
                         :cljs (js/JSON.stringify (clj->js data))))
        enc-ops (get-ops-per-sec enc-fn num-ops)
        json-enc-ops (get-ops-per-sec json-enc-fn num-ops)
        encoded (enc-fn)
        json-encoded (json-enc-fn)
        dec-fn #(deserialize-same add-to-cart-rsp-schema
                                  encoded)
        json-dec-fn (fn []
                      #?(:clj (json/parse-string json-encoded true)
                         :cljs (js->clj (js/JSON.parse json-encoded))))
        dec-ops (get-ops-per-sec dec-fn num-ops)
        json-dec-ops (get-ops-per-sec json-dec-fn num-ops)]
    (infof "Encoding ops per sec: %.0f" enc-ops)
    (infof "Decoding ops per sec: %.0f" dec-ops)
    (infof "JSON Enc ops per sec: %.0f" json-enc-ops)
    (infof "JSON Dec ops per sec: %.0f" json-dec-ops)
    (infof "Encoded size: %d" (count encoded))
    (infof "JSON Enc size: %d" (count json-encoded))
    (is (< #?(:cljs 20000 :clj 200000) enc-ops))
    (is (< #?(:cljs 40000 :clj 300000) dec-ops))))
