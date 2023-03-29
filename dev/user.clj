(ns user
  (:require
   [clojure.string :as str]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [taoensso.timbre :as log]))

(def string-schema l/string-schema)
(def array-int-schema (l/array-schema l/int-schema))
(l/def-record-schema record-int-schema [:a l/int-schema])
(l/def-record-schema record-string-schema [:b l/string-schema])
(l/def-record-schema record-array-int-schema [:c (l/array-schema l/int-schema)])
(l/def-record-schema record-array-record-int-schema [:d (l/array-schema record-int-schema)])

(defn gen-str [n]
  (str/join "" (range n)))

(defn gen-array-int [n]
  (range n))

(defn gen-record-str [n]
  {:b (gen-str n)})

(defn gen-record-array-int [n]
  {:c (gen-array-int n)})

(defn gen-record-array-record-int [n]
  {:d (for [x (range n)] {:a x})})

(defn time-it [schema gen]
  (doseq [n [1 10 100 1000 10000 100000 1000000]]
    (let [data (gen n)]
      (time (l/deserialize-same schema (l/serialize schema data))))))

(comment
  (time-it string-schema gen-str)
  ; 0.080443
  ; 0.082696
  ; 0.050982
  ; 0.067877
  ; 0.261435
  ; 1.174952
  ; 5.901504

  (time-it array-int-schema gen-array-int)
  ; 0.11724
  ; 0.13978
  ; 0.133843
  ; 0.71462
  ; 6.81258
  ; 56.043202
  ; 312.860248

  (time-it record-string-schema gen-record-str)
  ; 0.158773
  ; 0.144422
  ; 0.172473
  ; 0.170699
  ; 0.176788
  ; 0.529249
  ; 10.922078

  (time-it record-array-int-schema gen-record-array-int)
  ; 0.128098
  ; 0.11545
  ; 0.087746
  ; 0.631218
  ; 4.990566
  ; 47.52845
  ; 315.632594

  (time-it record-array-record-int-schema gen-record-array-record-int)
  ; 0.109056
  ; 0.110355
  ; 0.175876
  ; 1.677482
  ; 14.014604
  ; 173.875684
  ; 1215.830282

  )
