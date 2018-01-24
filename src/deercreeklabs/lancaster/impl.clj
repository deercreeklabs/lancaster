(ns deercreeklabs.lancaster.impl
  (:require
   [cheshire.core :as json]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof warnf]])
  (:import
   (com.google.common.io LittleEndianDataInputStream
                         LittleEndianDataOutputStream)
   (java.io ByteArrayInputStream ByteArrayOutputStream)
   (java.math BigInteger)))

(primitive-math/use-primitive-operators)

(defrecord OutputStream [baos ledos]
  u/IOutputStream
  (write-byte [this b]
    (.writeByte ^LittleEndianDataOutputStream ledos b))

  (write-bytes [this ba num-bytes]
    (.write ^LittleEndianDataOutputStream ledos ba 0 num-bytes))

  (write-bytes-w-len-prefix [this ba]
    (let [num-bytes (count ba)]
      (u/write-long-varint-zz this num-bytes)
      (u/write-bytes this ba num-bytes)))

  (write-utf8-string [this s]
    (let [bytes (.getBytes ^String s "UTF-8")
          num-bytes (count bytes)]
      (u/write-long-varint-zz this num-bytes)
      (u/write-bytes this bytes num-bytes)))

  (write-float [this f]
    (.writeFloat ^LittleEndianDataOutputStream ledos ^float f))

  (write-double [this d]
    (.writeDouble ^LittleEndianDataOutputStream ledos ^double d))

  (to-byte-array [this]
    (.toByteArray ^ByteArrayOutputStream baos)))

(defn make-output-stream
  [initial-size]
  (let [baos (ByteArrayOutputStream. initial-size)
        ledos (LittleEndianDataOutputStream. baos)]
    (->OutputStream baos ledos)))

(defn output-stream->byte-array [^ByteArrayOutputStream os]
  (.toByteArray os))

(defrecord InputStream [bais ledis]
  u/IInputStream
  (read-long-varint-zz [this]
    (loop [i 0
           out 0]
      (let [b (.readByte ^LittleEndianDataInputStream ledis)]
        (if (zero? (bit-and b 0x80))
          (let [zz-n (-> (bit-shift-left b i)
                         (bit-or out))
                long-out (->> (bit-and zz-n 1)
                              (- 0)
                              (bit-xor (unsigned-bit-shift-right zz-n 1)))]
            long-out)
          (let [out (-> (bit-and b 0x7f)
                        (bit-shift-left i)
                        (bit-or out))
                i (+ 7 i)]
            (if (<= i 63)
              (recur i out)
              (throw (ex-info "Variable-length quantity is more than 64 bits"
                              (u/sym-map i)))))))))

  (read-byte [this]
    (.readByte ^LittleEndianDataInputStream ledis))

  (read-bytes [this num-bytes]
    (let [ba (byte-array num-bytes)]
      (.readFully ^LittleEndianDataInputStream ledis #^bytes ba 0 num-bytes)
      ba))

  (read-len-prefixed-bytes [this]
    (let [num-bytes (u/read-long-varint-zz this)]
      (u/read-bytes this num-bytes)))

  (read-utf8-string [this]
    (String. #^bytes (u/read-len-prefixed-bytes this) "UTF-8"))

  (read-float [this]
    (.readFloat ^LittleEndianDataInputStream ledis))

  (read-double [this]
    (.readDouble ^LittleEndianDataInputStream ledis)))

(defn make-input-stream [ba]
  (let [bais (ByteArrayInputStream. ba)
        ledis (LittleEndianDataInputStream. bais)]
    (->InputStream bais ledis)))
