(ns deercreeklabs.lancaster
  (:require
   [camel-snake-kebab.core :as csk]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster)))

#?(:cljs
   (set! *warn-on-infer* true))

;;;;;;;;;;;;;;;;;;;; Primitive Schemas ;;;;;;;;;;;;;;;;;;;;

(def null-schema (schemas/make-primitive-schema :null))
(def boolean-schema (schemas/make-primitive-schema :boolean))
(def int-schema (schemas/make-primitive-schema :int))
(def long-schema (schemas/make-primitive-schema :long))
(def float-schema (schemas/make-primitive-schema :float))
(def double-schema (schemas/make-primitive-schema :double))
(def bytes-schema (schemas/make-primitive-schema :bytes))
(def string-schema (schemas/make-primitive-schema :string))

;;;;;;;;;;;;;;;;;;;; Schema Macros ;;;;;;;;;;;;;;;;;;;;

(defmacro def-record-schema
  [schema-name & fields]
  `(schemas/schema-helper :record ~schema-name ~(vec fields)))

(defmacro def-enum-schema
  [schema-name & symbols]
  `(schemas/schema-helper :enum ~schema-name ~(vec symbols)))

(defmacro def-fixed-schema
  [schema-name size]
  `(schemas/schema-helper :fixed ~schema-name ~size))

(defmacro def-array-schema
  [schema-name items-schema]
  `(schemas/schema-helper :array ~schema-name ~items-schema))

(defmacro def-map-schema
  [schema-name values-schema]
  `(schemas/schema-helper :map ~schema-name ~values-schema))

(defmacro def-union-schema
  [schema-name & member-schemas]
  `(schemas/schema-helper :union ~schema-name ~(vec member-schemas)))

;;;;;;;;;;;;;;;;;;;; Recursion Schema ;;;;;;;;;;;;;;;;;;;;


(def nil-or-recur-schema
  "This is a special schema that can only be used inside record fields."
  :__nil_or_recur_schema__)

;;;;;;;;;;;;;;;;;;;; API Fns ;;;;;;;;;;;;;;;;;;;;

(defn serialize [schema-obj data]
  (schemas/serialize schema-obj data))

(defn deserialize
  ([reader-schema-obj writer-json-schema ba]
   (schemas/deserialize reader-schema-obj writer-json-schema ba false))
  ([reader-schema-obj writer-json-schema ba return-java?]
   (schemas/deserialize reader-schema-obj writer-json-schema ba return-java?)))

(defn wrap [schema data]
  (schemas/wrap schema data))

(defn unwrap [wrapped-data]
  (first (vals wrapped-data)))

(defn get-edn-schema [schema]
  (schemas/get-edn-schema schema))

(defn get-json-schema [schema]
  (schemas/get-json-schema schema))

(defn get-parsing-canonical-form [schema]
  (schemas/get-parsing-canonical-form schema))

(defn get-fingerprint128 [schema]
  (schemas/get-fingerprint128 schema))
