(ns deercreeklabs.lancaster.impl
  (:require
   [cljsjs.bytebuffer]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]
   [text-encoding :as text]))

(set! *warn-on-infer* true)

(def ByteBuffer js/ByteBuffer)

(def encoder (text/TextEncoder.))
(def decoder (text/TextDecoder. "utf-8"))

(defprotocol IResize
  (embiggen [this min-added-bytes]))

(deftype OutputStream [^:mutable ba ^:mutable buflen ^:mutable pos]
  u/IOutputStream
  (write-byte [this b]
    (let [new-pos (inc pos)]
      (when (= new-pos buflen)
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
    (u/write-bytes-w-len-prefix this (js/Int8Array. (.encode encoder s))))

  (write-float [this f]
    (let [new-pos (+ pos 4)
          dataview (js/DataView. (goog.object/get ba "buffer"))]
      (when (= new-pos buflen)
        (embiggen this 4))
      (.setFloat32 dataview pos f true)
      (set! pos new-pos)))

  (write-double [this d]
    (let [new-pos (+ pos 8)
          dataview (js/DataView. (goog.object/get ba "buffer"))]
      (when (= new-pos buflen)
        (embiggen this 8))
      (.setFloat64 dataview pos d true)
      (set! pos new-pos)))

  (to-byte-array [this]
    (.slice ba 0 pos))

  IResize
  (embiggen [this min-added-bytes]
    (let [num-new-bytes (if (> min-added-bytes buflen)
                          min-added-bytes
                          buflen)
          new-buf (ba/byte-array (+ buflen num-new-bytes))]
      (.set new-buf ba)
      (set! ba new-buf))))

(defn make-output-stream
  [initial-size]
  (->OutputStream (ba/byte-array initial-size) initial-size 0))

(defrecord InputStream [buf]
  u/IInputStream
  (read-long-varint-zz [this]
    (.readVarint64ZigZag buf))

  (read-byte [this]
    (.readInt8 buf))

  (read-bytes [this num-bytes]
    (let [start (.-offset ^js/ByteBuffer buf)
          end (+ start num-bytes)
          out (-> (.slice buf start end)
                  (.toArrayBuffer)
                  (js/Int8Array.))]
      (.skip ^js/ByteBuffer buf num-bytes)
      out))

  (read-len-prefixed-bytes [this]
    (let [num-bytes (.toInt (u/read-long-varint-zz this))]
      (u/read-bytes this num-bytes)))

  (read-utf8-string [this]
    (let [num-bytes (.toInt (u/read-long-varint-zz this))]
      (.readUTF8String buf num-bytes (.-METRICS_BYTES ByteBuffer))))

  (read-float [this]
    (.readFloat32 buf))

  (read-double [this]
    (.readFloat64 buf)))

(defn make-input-stream [ba]
  (->InputStream (.wrap ByteBuffer (.-buffer ^js/Int8Array ba)
                        "binary" true true)))
