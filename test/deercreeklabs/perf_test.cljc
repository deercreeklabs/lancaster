(ns deercreeklabs.perf-test
  (:require
   #?(:clj [cheshire.core :as json])
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [schema.core :as s :include-macros true]))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! false)

(l/def-record-schema add-to-cart-req-schema
  [:sku l/int-schema]
  [:qty-requested l/int-schema 0])

(l/def-enum-schema why-schema
  :all :stock :limit)

(l/def-fixed-schema a-fixed-schema 2)

(l/def-record-schema add-to-cart-rsp-schema
  [:qty-requested l/int-schema]
  [:qty-added l/int-schema]
  [:current-qty l/int-schema]
  [:req add-to-cart-req-schema]
  [:reason why-schema :stock]
  [:data a-fixed-schema]
  [:other-data l/bytes-schema])

(defn get-ops-per-sec [f iters]
  (let [start-ms (u/current-time-ms)
        _ (dotimes [_ iters]
            (f))
        ms (- (u/current-time-ms) start-ms)]
    (/ (* 1000 iters) ms)))

(deftest ^:perf test-serdes-speed
  (let [data {:qty-requested 123
              :qty-added 10
              :current-qty 10
              :req {:sku 123 :qty-requested 123}
              :reason :limit
              :data (ba/byte-array [66 67])
              :other-data (ba/byte-array [123 123])}
        num-ops #?(:cljs 1e4 :clj 1e5)
        enc-fn #(l/serialize add-to-cart-rsp-schema data)
        json-enc-fn (fn []
                      #?(:clj (json/generate-string data)
                         :cljs (js/JSON.stringify (clj->js data))))
        deflated-json-enc-fn #(-> (json-enc-fn)
                                  (ba/utf8->byte-array)
                                  (ba/deflate))
        enc-ops (get-ops-per-sec enc-fn num-ops)
        json-enc-ops (get-ops-per-sec json-enc-fn num-ops)
        deflated-json-enc-ops (get-ops-per-sec deflated-json-enc-fn
                                               (/ num-ops 10))
        encoded (enc-fn)
        json-encoded (json-enc-fn)
        deflated-json-encoded (deflated-json-enc-fn)
        dec-fn #(l/deserialize-same add-to-cart-rsp-schema
                                    encoded)
        json-dec-fn (fn []
                      #?(:clj (json/parse-string json-encoded true)
                         :cljs (js->clj (js/JSON.parse json-encoded))))
        deflated-json-dec-fn (fn []
                               (let [json (-> deflated-json-encoded
                                              (ba/inflate)
                                              (ba/byte-array->utf8))]
                                 #?(:clj (json/parse-string json true)
                                    :cljs (js->clj (js/JSON.parse json)))))
        deflated-json-dec-ops (get-ops-per-sec deflated-json-dec-fn
                                               (/ num-ops 10))
        dec-ops (get-ops-per-sec dec-fn num-ops)
        json-dec-ops (get-ops-per-sec json-dec-fn num-ops)
        floor #?(:cljs Math/floor
                 :clj #(Math/floor (double %)))]
    (str "Lancaster encode ops per sec:     " (floor enc-ops))
    (str "Lancaster decode ops per sec:     " (floor dec-ops))
    (str "JSON encode ops per sec:          " (floor json-enc-ops))
    (str "JSON decode ops per sec:          " (floor json-dec-ops))
    (str "Deflated JSON encode ops per sec: " (floor deflated-json-enc-ops))
    (str "Deflated JSON decode ops per sec: " (floor deflated-json-dec-ops))
    (str "Lancaster encoded size:           " (count encoded))
    (str "JSON encoded size:                " (count json-encoded))
    (str "Deflated JSON encoded size:       " (count deflated-json-encoded))
    (is (< #?(:cljs 20000 :clj 200000) enc-ops))
    (is (< #?(:cljs 40000 :clj 300000) dec-ops))))
