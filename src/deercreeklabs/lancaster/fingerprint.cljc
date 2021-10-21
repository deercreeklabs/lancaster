(ns deercreeklabs.lancaster.fingerprint
  #?(:cljs
     (:refer-clojure :exclude [bit-and bit-xor unsigned-bit-shift-right]))
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm])
   [schema.core :as s :include-macros true])
  #?(:cljs
     (:import
      (goog.math Long))))

(s/defn make-long :- Long
  [x :- s/Any]
  (when-not (nil? x)
    #?(:clj (clojure.core/long x)
       :cljs (if (u/long? x)
               x
               (Long.fromNumber x)))))


;; Based on
;; http://avro.apache.org/docs/current/spec.html#schema_fingerprints

(def seed (u/str->long "-4513414715797952619"))
(def long-one (make-long 1))

#?(:cljs (def class type))

#?(:cljs (defn bit-and [l other]
           (.and l other)))

#?(:cljs (defn unsigned-bit-shift-right [l num-bits]
           (.shiftRightUnsigned ^Long l num-bits)))

#?(:cljs (defn bit-xor [l other]
           (.xor l other)))

(defn negate [l]
  #?(:clj (unchecked-negate l)
     :cljs (.negate ^Long l)))

(defn calc-fp [i]
  (let [f (fn [fp j]
            (let [fp (make-long fp)
                  mask (make-long (negate (bit-and fp long-one)))]
              (bit-xor (unsigned-bit-shift-right fp 1)
                       (bit-and seed mask))))]
    (reduce f (make-long i) (range 8))))

(def fingerprint-table
  (let [f (fn [fp-table i]
            (conj fp-table (calc-fp i)))]
    (reduce f [] (range 256))))

(s/defn fingerprint64 :- Long
  [s :- s/Str]
  (let [ba (ba/utf8->byte-array s)
        f (fn [acc b]
            (let [b (make-long b)
                  acc (make-long acc)]
              (bit-xor (unsigned-bit-shift-right acc 8)
                       (fingerprint-table (int (bit-and (bit-xor acc b)
                                                        (make-long 255)))))))
        num-bytes (count ba)]
    (loop [acc seed
           i 0]
      (if (= num-bytes i)
        acc
        (let [b (aget #^bytes ba i)]
          (recur (f acc b)
                 (inc i)))))))

(s/defn fingerprint128 :- ba/ByteArray
  [s :- s/Str]
  (->  (ba/utf8->byte-array s)
       (ba/md5)))

(s/defn fingerprint256 :- ba/ByteArray
  [s :- s/Str]
  (->  (ba/utf8->byte-array s)
       (ba/sha256)))
