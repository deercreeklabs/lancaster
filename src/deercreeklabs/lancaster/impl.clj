(ns deercreeklabs.lancaster.impl
  (:require
   [cheshire.core :as json]
   [deercreeklabs.lancaster.utils :as u])
  (:import
   (com.google.common.io LittleEndianDataInputStream
                         LittleEndianDataOutputStream)
   (java.io ByteArrayInputStream ByteArrayOutputStream)
   (java.math BigInteger)))

(set! *warn-on-reflection* true)
(clj-commons.primitive-math/use-primitive-operators)

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

(defn output-stream
  [initial-size]
  (let [baos (ByteArrayOutputStream. initial-size)
        ledos (LittleEndianDataOutputStream. baos)]
    (->OutputStream baos ledos)))

(defn output-stream->byte-array [^ByteArrayOutputStream os]
  (.toByteArray os))

(defrecord InputStream [^ByteArrayInputStream bais
                        ^LittleEndianDataInputStream ledis]
  u/IInputStream
  (mark [this]
    (.mark ledis (.available ledis)))

  (read-byte [this]
    (.readByte ledis))

  (read-bytes [this num-bytes]
    (let [ba (byte-array num-bytes)]
      (.readFully ledis #^bytes ba 0 num-bytes)
      ba))

  (read-len-prefixed-bytes [this]
    (let [num-bytes (u/read-long-varint-zz this)]
      (u/read-bytes this num-bytes)))

  (read-utf8-string [this]
    (String. #^bytes (u/read-len-prefixed-bytes this) "UTF-8"))

  (read-float [this]
    (.readFloat ledis))

  (read-double [this]
    (.readDouble ledis))

  (reset-to-mark! [this]
    (.reset ledis)))

(defn input-stream [ba]
  (let [bais (ByteArrayInputStream. ba)
        ledis (LittleEndianDataInputStream. bais)]
    (->InputStream bais ledis)))
