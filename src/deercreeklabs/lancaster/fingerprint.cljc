(ns deercreeklabs.lancaster.fingerprint
  #?(:cljs
     (:refer-clojure :exclude [bit-and bit-xor unsigned-bit-shift-right]))
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm]))
  #?(:cljs
     (:import
      (goog.math Long))))

#?(:clj (set! *warn-on-reflection* true))

(defn make-long
  [x]
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

(defn ba->fp64 [ba]
  (let [f (fn [acc b]
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

(defn fingerprint64
  [s]
  (->  (ba/utf8->byte-array s)
       (ba->fp64)))

(defn fingerprint128
  [s]
  (->  (ba/utf8->byte-array s)
       (ba/md5)))

(defn fingerprint256
  [s]
  (->  (ba/utf8->byte-array s)
       (ba/sha256)))
