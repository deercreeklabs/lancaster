(ns deercreeklabs.lancaster.impl
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(set! *warn-on-infer* true)

(defprotocol IResize
  (embiggen [this min-added-bytes]))

(deftype OutputStream [^:mutable ba ^:mutable buflen ^:mutable pos]
  u/IOutputStream
  (write-byte [this b]
    (let [new-pos (inc pos)]
      (when (>= new-pos buflen)
        (embiggen this 1))
      (aset ba pos b)
      (set! pos new-pos)))

  (write-bytes [this source-ba num-bytes]
    (let [new-pos (+ pos num-bytes)]
      (when (>= new-pos buflen)
        (embiggen this num-bytes))
      (.set ba source-ba pos)
      (set! pos new-pos)))

  (write-bytes-w-len-prefix [this source-ba]
    (let [num-bytes (count source-ba)]
      (u/write-long-varint-zz this num-bytes)
      (u/write-bytes this source-ba num-bytes)))

  (write-utf8-string [this s]
    (u/write-bytes-w-len-prefix this (ba/utf8->byte-array s)))

  (write-float [this f]
    (let [new-pos (+ pos 4)
          dataview (js/DataView. (goog.object/get ba "buffer"))]
      (when (>= new-pos buflen)
        (embiggen this 4))
      (.setFloat32 dataview pos f true)
      (set! pos new-pos)))

  (write-double [this d]
    (let [new-pos (+ pos 8)
          dataview (js/DataView. (goog.object/get ba "buffer"))]
      (when (>= new-pos buflen)
        (embiggen this 8))
      (.setFloat64 dataview pos d true)
      (set! pos new-pos)))

  (to-byte-array [this]
    (.slice ba 0 pos))

  IResize
  (embiggen [this min-added-bytes]
    (let [num-new-bytes (max min-added-bytes buflen)
          new-buf-len (+ buflen num-new-bytes)
          new-buf (ba/byte-array new-buf-len)]
      (.set new-buf ba)
      (set! buflen new-buf-len)
      (set! ba new-buf))))

(defn make-output-stream
  [initial-size]
  (->OutputStream (ba/byte-array initial-size) initial-size 0))

(defrecord InputStream [ba ^:mutable pos ^:mutable mark-pos]
  u/IInputStream
  (mark [this]
    (set! mark-pos pos))

  (read-byte [this]
    (let [b (aget ba pos)]
      (set! pos (inc pos))
      b))

  (read-bytes [this num-bytes]
    (let [new-pos (+ pos num-bytes)
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
    (let [bs (u/read-bytes this 4)
          dataview (js/DataView. (goog.object/get bs "buffer"))]
      (.getFloat32 dataview 0 true)))

  (read-double [this]
    (let [bs (u/read-bytes this 8)
          array-buf (goog.object/get bs "buffer")
          dataview (js/DataView. array-buf)]
      (.getFloat64 dataview 0 true)))

  (reset-to-mark! [this]
    (set! pos mark-pos)))

(defn make-input-stream [ba]
  (->InputStream ba 0 0))
