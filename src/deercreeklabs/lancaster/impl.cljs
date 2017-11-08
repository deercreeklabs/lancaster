(ns deercreeklabs.lancaster.impl
  (:require
   [cljsjs.bytebuffer]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(set! *warn-on-infer* true)

(def ByteBuffer js/ByteBuffer)

(defrecord OutputStream [buf]
  u/IOutputStream
  (write-long-varint-zz [this l]
    (.writeVarint64ZigZag buf l))

  (write-byte [this b]
    (.writeInt8 buf b))

  (write-bytes [this ba num-bytes]
    (.append buf (.-buffer ^js/Int8Array ba)))

  (write-bytes-w-len-prefix [this ba]
    (let [num-bytes (count ba)]
      (u/write-long-varint-zz this num-bytes)
      (u/write-bytes this ba num-bytes)))

  (write-utf8-string [this s]
    (let [num-bytes (.calculateUTF8Bytes ByteBuffer s)]
      (u/write-long-varint-zz this num-bytes)
      (.writeUTF8String buf s)))

  (write-float [this f]
    (.writeFloat32 buf f))

  (write-double [this d]
    (.writeFloat64 buf d))

  (to-byte-array [this]
    (.flip buf)
    (-> (.toArrayBuffer buf)
        (js/Int8Array.))))

(defn make-output-stream
  [initial-size]
  (->OutputStream (ByteBuffer. initial-size true)))

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
