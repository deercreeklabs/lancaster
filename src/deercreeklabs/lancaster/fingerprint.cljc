(ns deercreeklabs.lancaster.fingerprint
  #?(:cljs
     (:refer-clojure :exclude [bit-and bit-xor unsigned-bit-shift-right]))
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:cljs [cljsjs.bytebuffer])
   [schema.core :as s :include-macros true]))

#?(:cljs (def Long js/Long))

(s/defn long :- Long
  [x :- s/Any]
  (when-not (nil? x)
    #?(:clj (clojure.core/long x)
       :cljs (Long.fromValue x))))


;; Based on
;; http://avro.apache.org/docs/current/spec.html#Schema+Fingerprints

(def seed (u/hex-str->long "c15d213aa4d7a795"))
(def long-one (long 1))

#?(:cljs (def class type))

#?(:cljs (defn bit-and [l other]
           (.and l other)))

#?(:cljs (defn unsigned-bit-shift-right [l num-bits]
           (.shiftRightUnsigned l num-bits)))

#?(:cljs (defn bit-xor [l other]
           (.xor l other)))

(defn negate [l]
  #?(:clj (unchecked-negate l) :cljs (.negate l)))

(defn calc-fp [i]
  (let [f (fn [fp j]
            (let [fp (long fp)
                  mask (long (negate (bit-and fp long-one)))]
              (bit-xor (unsigned-bit-shift-right fp 1)
                       (bit-and seed mask))))]
    (reduce f (long i) (range 8))))

(def fingerprint-table
  (let [f (fn [fp-table i]
            (conj fp-table (calc-fp i)))]
    (reduce f [] (range 256))))

(s/defn fingerprint64 :- Long
  [s :- s/Str]
  (let [ba (ba/utf8->byte-array s)
        f (fn [acc b]
            (let [b (byte b)
                  acc (long acc)]
              (bit-xor (unsigned-bit-shift-right acc 8)
                       (fingerprint-table (int (bit-and (bit-xor acc b)
                                                        0xff))))))
        num-bytes (count ba)]
    (loop [acc seed
           i 0]
      (if (= num-bytes i)
        acc
        (let [b (aget #^bytes ba i)]
          (recur (f acc b)
                 (inc i)))))))
