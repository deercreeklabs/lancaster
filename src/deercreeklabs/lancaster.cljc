(ns deercreeklabs.lancaster
  (:require
   [camel-snake-kebab.core :as csk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.impl :as i]
   #?(:clj [deercreeklabs.lancaster.schemas :as schemas])
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [deercreeklabs.lancaster :refer [def-primitive-schema]])))

#?(:cljs
   (set! *warn-on-infer* true))

;;;;;;;;;;;;;;;;;;;; Schema Macros ;;;;;;;;;;;;;;;;;;;;

#?(:clj
   (defmacro def-record-schema [schema-name & args]
     (schemas/schema-helper :record schema-name (vec args))))

#?(:clj
   (defmacro def-enum-schema [schema-name & symbols]
     (schemas/schema-helper :enum schema-name (vec symbols))))

#?(:clj
   (defmacro def-fixed-schema [schema-name size]
     (schemas/schema-helper :fixed schema-name size)))

#?(:clj
   (defmacro def-array-schema [schema-name items-schema]
     (schemas/schema-helper :array schema-name items-schema)))

#?(:clj
   (defmacro def-map-schema [schema-name values-schema]
     (schemas/schema-helper :map schema-name values-schema)))

#?(:clj
   (defmacro def-union-schema [schema-name & member-schemas]
     (schemas/schema-helper :union schema-name (vec member-schemas))))

#?(:clj
   (defmacro def-primitive-schema [schema-name]
     (schemas/schema-helper :primitive schema-name
                            (keyword (u/drop-schema-from-name schema-name)))))

;;;;;;;;;;;;;;;;;;;; Recursion Schema ;;;;;;;;;;;;;;;;;;;;

(def nil-or-recur-schema
  "This is a special schema that can only be used inside record fields."
  :__nil_or_recur_schema__)

;;;;;;;;;;;;;;;;;;;; Primitive Schemas ;;;;;;;;;;;;;;;;;;;;

(def-primitive-schema null-schema)
(def-primitive-schema boolean-schema)
(def-primitive-schema int-schema)
(def-primitive-schema long-schema)
(def-primitive-schema float-schema)
(def-primitive-schema double-schema)
(def-primitive-schema bytes-schema)
(def-primitive-schema string-schema)

;;;;;;;;;;;;;;;;;;;; API Fns ;;;;;;;;;;;;;;;;;;;;

(s/defn serialize :- ba/ByteArray
  [schema-obj :- (s/protocol u/IAvroSchema)
   data :- s/Any]
  (u/serialize schema-obj data))

(s/defn deserialize :- s/Any
  ([reader-schema-obj :- (s/protocol u/IAvroSchema)
    writer-json-schema :- s/Str
    ba :- ba/ByteArray]
   (u/deserialize reader-schema-obj writer-json-schema ba false))
  ([reader-schema-obj :- (s/protocol u/IAvroSchema)
    writer-json-schema :- s/Str
    ba :- ba/ByteArray
    return-java? :- s/Bool]
   (u/deserialize reader-schema-obj writer-json-schema ba return-java?)))

(defn wrap [schema data]
  (u/wrap schema data))

(defn get-edn-schema [schema]
  (u/get-edn-schema schema))

(defn get-json-schema [schema]
  (u/get-json-schema schema))

(defn get-parsing-canonical-form [schema]
  (u/get-parsing-canonical-form schema))

(defn get-fingerprint128 [schema]
  (u/get-fingerprint128 schema))
