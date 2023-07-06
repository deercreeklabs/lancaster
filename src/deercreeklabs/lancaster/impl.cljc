(ns deercreeklabs.lancaster.impl
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.object :as googo]))
  #?(:clj
     (:import
      (java.util Arrays))))

#?(:clj (set! *warn-on-reflection* true))
#?(:clj (clj-commons.primitive-math/use-primitive-operators))

(defprotocol IResize
  (embiggen! [this min-added-bytes]))

(deftype OutputStream
    [^#?(:clj :unsynchronized-mutable :cljs :mutable) ba
     ^#?(:clj :unsynchronized-mutable :cljs :mutable) buflen
     ^#?(:clj :unsynchronized-mutable :cljs :mutable) pos]
  u/IOutputStream
  (write-byte [this b]
    (let [new-pos (inc (int pos))]
      (when (> new-pos (int buflen))
        (embiggen! this 1))
      (aset ^bytes ba (int pos) (byte b))
      (set! pos new-pos)))

  (write-bytes [this source-ba num-bytes]
    (let [new-pos (+ (int pos) (int num-bytes))]
      (when (> (int new-pos) (int buflen))
        (embiggen! this num-bytes))
      #?(:clj (System/arraycopy source-ba 0 ba pos num-bytes)
         :cljs (.set ba source-ba pos))
      (set! pos new-pos)))

  (write-bytes-w-len-prefix [this source-ba]
    (let [num-bytes (count source-ba)]
      (u/write-long-varint-zz this num-bytes)
      (u/write-bytes this source-ba num-bytes)))

  (write-utf8-string [this s]
    (u/write-bytes-w-len-prefix this (ba/utf8->byte-array s)))

  (write-float [this f]
    (let [new-pos (+ (int pos) 4)]
      (when (> new-pos (int buflen))
        (embiggen! this 4))
      #?(:clj (let [i (Float/floatToRawIntBits f)]
                (aset ^bytes ba pos
                      (unchecked-byte (bit-and i 0xff)))
                (aset ^bytes ba (+ (int pos) 1)
                      (unchecked-byte (bit-and (bit-shift-right i 8) 0xff)))
                (aset ^bytes ba (+ (int pos) 2)
                      (unchecked-byte (bit-and (bit-shift-right i 16) 0xff)))
                (aset ^bytes ba (+ (int pos) 3)
                      (unchecked-byte (bit-and (bit-shift-right i 24) 0xff))))
         :cljs (let [dataview (js/DataView. (googo/get ba "buffer"))]
                 (.setFloat32 dataview pos f true)))
      (set! pos new-pos)))

  (write-double [this d]
    (let [new-pos (+ (int pos) 8)]
      (when (> new-pos (int buflen))
        (embiggen! this 8))
      #?(:clj (let [l (Double/doubleToRawLongBits d)]
                (aset ^bytes ba pos
                      (unchecked-byte (bit-and l 0xff)))
                (aset ^bytes ba (+ (int pos) 1)
                      (unchecked-byte (bit-and (bit-shift-right l 8) 0xff)))
                (aset ^bytes ba (+ (int pos) 2)
                      (unchecked-byte (bit-and (bit-shift-right l 16) 0xff)))
                (aset ^bytes ba (+ (int pos) 3)
                      (unchecked-byte (bit-and (bit-shift-right l 24) 0xff)))
                (aset ^bytes ba (+ (int pos) 4)
                      (unchecked-byte (bit-and (bit-shift-right l 32) 0xff)))
                (aset ^bytes ba (+ (int pos) 5)
                      (unchecked-byte (bit-and (bit-shift-right l 40) 0xff)))
                (aset ^bytes ba (+ (int pos) 6)
                      (unchecked-byte (bit-and (bit-shift-right l 48) 0xff)))
                (aset ^bytes ba (+ (int pos) 7)
                      (unchecked-byte (bit-and (bit-shift-right l 56) 0xff))))
         :cljs (let [dataview (js/DataView. (googo/get ba "buffer"))]
                 (.setFloat64 dataview pos d true)))
      (set! pos new-pos)))

  (to-byte-array [this]
    #?(:clj (Arrays/copyOfRange ^bytes ba (int 0) (int pos))
       :cljs (.slice ^js/Int8Array ba 0 pos)))

  IResize
  (embiggen! [this min-added-bytes]
    (let [num-new-bytes (max (int min-added-bytes) (int buflen))
          new-buf-len (+ (int buflen) num-new-bytes)
          new-buf (ba/byte-array new-buf-len)]
      #?(:clj (System/arraycopy ba 0 new-buf 0 buflen)
         :cljs (.set ^js/Int8Array new-buf ^js/Int8Array ba))
      (set! buflen new-buf-len)
      (set! ba new-buf))))

(defn output-stream
  [initial-size]
  (->OutputStream (ba/byte-array initial-size) initial-size 0))

(deftype InputStream
    [^#?(:clj :unsynchronized-mutable :cljs :mutable) ba
     ^#?(:clj :unsynchronized-mutable :cljs :mutable) pos
     ^#?(:clj :unsynchronized-mutable :cljs :mutable) mark-pos]
  u/IInputStream
  (mark [this]
    (set! mark-pos pos))

  (read-byte [this]
    (let [b (aget ^bytes ba pos)]
      (set! pos (+ (int pos) 1))
      b))

  (read-bytes [this num-bytes]
    (let [new-pos (+ (int pos) (int num-bytes))
          bs (ba/slice-byte-array ba pos new-pos)]
      (set! pos new-pos)
      bs))

  (read-len-prefixed-bytes [this]
    (let [num-bytes (u/read-long-varint-zz this)]
      (u/read-bytes this num-bytes)))

  (read-utf8-string [this]
    (let [num-bytes (u/read-long-varint-zz this)
          bytes (u/read-bytes this num-bytes)]
      (ba/byte-array->utf8 bytes)))

  (read-float [this]
    (let [bs (u/read-bytes this 4)]
      #?(:clj (let [i (bit-or (bit-and 0xff (aget ^bytes bs 0))
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 1))
                                              8)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 2))
                                              16)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 3))
                                              24))]
                (Float/intBitsToFloat i))
         :cljs (let [dataview (js/DataView. (googo/get bs "buffer"))]
                 (.getFloat32 dataview 0 true)))))

  (read-double [this]
    (let [bs (u/read-bytes this 8)]
      #?(:clj (let [l (bit-or (bit-and 0xff (aget ^bytes bs 0))
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 1))
                                              8)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 2))
                                              16)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 3))
                                              24)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 4))
                                              32)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 5))
                                              40)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 6))
                                              48)
                              (bit-shift-left (bit-and 0xff (aget ^bytes bs 7))
                                              56))]
                (Double/longBitsToDouble l))
         :cljs (let [dataview (js/DataView. (googo/get bs "buffer"))]
                 (.getFloat64 dataview 0 true)))))

  (reset-to-mark! [this]
    (set! pos mark-pos)))

(defn input-stream [ba]
  (->InputStream ba 0 0))
