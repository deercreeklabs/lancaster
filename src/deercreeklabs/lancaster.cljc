(ns deercreeklabs.lancaster
  (:require
   [camel-snake-kebab.core :as csk]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster)))

#?(:cljs
   (set! *warn-on-infer* true))


;;;;;;;;;;;;;;;;;;;; Schema Macros ;;;;;;;;;;;;;;;;;;;;

(defmacro def-record-schema
  [schema-name & fields]
  `(schemas/named-schema-helper* :record ~schema-name ~(vec fields)))

(defmacro def-enum-schema
  [schema-name & symbols]
  `(schemas/named-schema-helper* :enum ~schema-name ~(vec symbols)))

(defmacro def-fixed-schema
  [schema-name size]
  `(schemas/named-schema-helper* :fixed ~schema-name ~size))

;;;;;;;;;;;;;;;;;;;; API Fns ;;;;;;;;;;;;;;;;;;;;

(defn serialize [writer-schema data]
  (schemas/serialize writer-schema data))

(defn deserialize
  ([reader-schema writer-schema ba]
   (schemas/deserialize reader-schema writer-schema ba false))
  ([reader-schema writer-schema ba return-java?]
   (schemas/deserialize reader-schema writer-schema ba return-java?)))

(defn get-edn-schema [schema]
  (schemas/get-edn-schema schema))

(defn get-json-schema [schema]
  (schemas/get-json-schema schema))

(defn get-canonical-parsing-form [schema]
  (schemas/get-canonical-parsing-form schema))

(defn get-fingerprint128 [schema]
  (schemas/get-fingerprint128 schema))
