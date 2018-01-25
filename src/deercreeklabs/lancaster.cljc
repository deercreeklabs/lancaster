(ns deercreeklabs.lancaster
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:cljs (def Long js/Long))

(def int-schema (schemas/make-primitive-schema :int))
(def null-schema (schemas/make-primitive-schema :null))
(def boolean-schema (schemas/make-primitive-schema :boolean))
(def long-schema (schemas/make-primitive-schema :long))
(def float-schema (schemas/make-primitive-schema :float))
(def double-schema (schemas/make-primitive-schema :double))
(def bytes-schema (schemas/make-primitive-schema :bytes))
(def string-schema (schemas/make-primitive-schema :string))

(s/defn make-record-schema :- (s/protocol schemas/IAvroSchema)
  [name-kw :- s/Keyword
   fields :- [schemas/RecordFieldDef]]
  (schemas/make-schema :record name-kw fields))

(s/defn make-enum-schema :- (s/protocol schemas/IAvroSchema)
  [name-kw :- s/Keyword
   symbols :- [s/Keyword]]
  (schemas/make-schema :enum name-kw symbols))

(s/defn make-fixed-schema :- (s/protocol schemas/IAvroSchema)
  [name-kw :- s/Keyword
   size :- s/Int]
  (schemas/make-schema :fixed name-kw size))

(s/defn make-array-schema :- (s/protocol schemas/IAvroSchema)
  [items-schema :- (s/protocol schemas/IAvroSchema)]
  (schemas/make-schema :array nil items-schema))

(s/defn make-map-schema :- (s/protocol schemas/IAvroSchema)
  [values-schema :- (s/protocol schemas/IAvroSchema)]
  (schemas/make-schema :map nil values-schema))

(s/defn make-union-schema :- (s/protocol schemas/IAvroSchema)
  [members :- [(s/protocol schemas/IAvroSchema)]]
  (schemas/make-schema :union nil members))

(s/defn serialize :- ba/ByteArray
  [schema-obj :- (s/protocol schemas/IAvroSchema)
   data :- s/Any]
  ;; TODO: Figure out how to set initial size better
  (let [os (impl/make-output-stream 100)]
    (schemas/serialize schema-obj os data)
    (u/to-byte-array os)))

(s/defn deserialize :- s/Any
  [reader-schema-obj :- (s/protocol schemas/IAvroSchema)
   writer-pcf :- s/Str
   ba :- ba/ByteArray]
  (let [is (impl/make-input-stream ba)]
    (schemas/deserialize reader-schema-obj writer-pcf is)))

(s/defn wrap :- schemas/WrappedData
  [schema :- (s/protocol schemas/IAvroSchema)
   data :- s/Any]
  (schemas/wrap schema data))

(s/defn get-edn-schema :- s/Any
  [schema :- (s/protocol schemas/IAvroSchema)]
  (schemas/get-edn-schema schema))

(s/defn get-json-schema :- s/Str
  [schema :- (s/protocol schemas/IAvroSchema)]
  (schemas/get-json-schema schema))

(s/defn get-parsing-canonical-form :- s/Str
  [schema :- (s/protocol schemas/IAvroSchema)]
  (schemas/get-parsing-canonical-form schema))

(s/defn get-fingerprint64 :- Long
  [schema :- (s/protocol schemas/IAvroSchema)]
  (schemas/get-fingerprint64 schema))
