(ns deercreeklabs.lancaster
  (:require
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:cljs [goog.math :as gm])
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros deercreeklabs.lancaster)))

(def LancasterSchema (s/protocol u/ILancasterSchema))
(def LancasterSchemaOrNameKW (s/if keyword?
                               s/Keyword
                               LancasterSchema))
#?(:cljs
   (def Long gm/Long))

(def int-schema (schemas/primitive-schema :int))
(def null-schema (schemas/primitive-schema :null))
(def boolean-schema (schemas/primitive-schema :boolean))
(def long-schema (schemas/primitive-schema :long))
(def float-schema (schemas/primitive-schema :float))
(def double-schema (schemas/primitive-schema :double))
(def bytes-schema (schemas/primitive-schema :bytes))
(def string-schema (schemas/primitive-schema :string))

(s/defn record-schema :- LancasterSchema
  [name-kw :- s/Keyword
   fields :- [schemas/RecordFieldDef]]
  (schemas/schema :record name-kw fields))

(s/defn enum-schema :- LancasterSchema
  [name-kw :- s/Keyword
   symbols :- [s/Keyword]]
  (schemas/schema :enum name-kw symbols))

(s/defn fixed-schema :- LancasterSchema
  [name-kw :- s/Keyword
   size :- s/Int]
  (schemas/schema :fixed name-kw size))

(s/defn flex-map-schema :- LancasterSchema
  [name-kw :- s/Keyword
   keys-schema :- LancasterSchema
   values-schema :- LancasterSchema]
  (schemas/schema :flex-map name-kw [keys-schema values-schema]))

(s/defn array-schema :- LancasterSchema
  [items-schema :- LancasterSchema]
  (schemas/schema :array nil items-schema))

(s/defn map-schema :- LancasterSchema
  [values-schema :- LancasterSchema]
  (schemas/schema :map nil values-schema))

(s/defn union-schema :- LancasterSchema
  [members :- [LancasterSchemaOrNameKW]]
  (schemas/schema :union nil members))

(s/defn merge-record-schemas :- LancasterSchema
  [name-kw :- s/Keyword
   schemas :- [LancasterSchema]]
  (schemas/merge-record-schemas name-kw schemas))

(s/defn maybe :- LancasterSchema
  [schema :- LancasterSchemaOrNameKW]
  (union-schema [null-schema schema]))

(s/defn serialize :- ba/ByteArray
  [schema-obj :- LancasterSchema
   data :- s/Any]
  (when-not (satisfies? u/ILancasterSchema schema-obj)
    (throw
     (ex-info (str "First argument to serialize must be a schema "
                   "object. The object must satisfy the ILancasterSchema "
                   "protocol.")
              {:schema-obj schema-obj
               :schema-obj-type (#?(:clj class :cljs type) schema-obj)})))
  (u/serialize schema-obj data))

(s/defn deserialize :- s/Any
  [reader-schema-obj :- LancasterSchema
   writer-pcf :- s/Str
   ba :- ba/ByteArray]
  (when-not (satisfies? u/ILancasterSchema reader-schema-obj)
    (throw
     (ex-info (str "First argument to deserialize must be a schema "
                   "object representing the reader's schema. The object "
                   "must satisfy the ILancasterSchema protocol.")
              {:reader-schema-obj reader-schema-obj
               :reader-schema-obj-type
               (#?(:clj class :cljs type) reader-schema-obj)})))
  (when-not (string? writer-pcf)
    (throw
     (ex-info (str "Second argument to deserialize must be a string "
                   "representing the parsing canonical form of the "
                   "writer's schema.")
              {:writer-pcf writer-pcf
               :writer-pcf-type (#?(:clj class :cljs type) writer-pcf)})))
  (when-not (instance? ba/ByteArray ba)
    (throw (ex-info "Third argument to deserialize must be a byte array."
                    {:ba ba
                     :ba-type (#?(:clj class :cljs type) ba)})))
  (let [is (impl/input-stream ba)]
    (u/deserialize reader-schema-obj writer-pcf is)))

(s/defn wrap :- schemas/WrappedData
  [schema :- LancasterSchema
   data :- s/Any]
  (u/wrap schema data))

(s/defn edn-schema :- s/Any
  [schema :- LancasterSchema]
  (u/edn-schema schema))

(s/defn json-schema :- s/Str
  [schema :- LancasterSchema]
  (u/json-schema schema))

(s/defn plumatic-schema :- s/Any
  [schema :- LancasterSchema]
  (u/plumatic-schema schema))

(s/defn pcf :- s/Str
  [schema :- LancasterSchema]
  (u/parsing-canonical-form schema))

(s/defn fingerprint64 :- Long
  [schema :- LancasterSchema]
  (u/fingerprint64 schema))

(s/defn schema? :- s/Bool
  [arg :- s/Any]
  (satisfies? u/ILancasterSchema arg))

(s/defn default-data :- s/Any
  [schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info "Argument to default-data must be a schema object."
              {:given-arg schema})))
  (u/default-data (edn-schema schema)))

;;;;;;;;;; Named Schema Helper Macros ;;;;;;;;;;;;;;;;

(defmacro def-record-schema
  [clj-name & fields]
  (when-not (pos? (count fields))
    (throw
     (ex-info "Missing record fields in def-record-schema."
              (u/sym-map clj-name fields))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)]
    `(def ~clj-name
       (schemas/schema :record ~ns-name ~schema-name (vector ~@fields)))))

(defmacro def-enum-schema
  [clj-name & symbols]
  (when-not (pos? (count symbols))
    (throw
     (ex-info "Missing symbols sequence in def-enum-schema."
              (u/sym-map clj-name symbols))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)]
    `(def ~clj-name
       (schemas/schema :enum ~ns-name ~schema-name (vector ~@symbols)))))

(defmacro def-fixed-schema
  [clj-name size]
  (when-not (and (pos? size) (integer? size))
    (throw
     (ex-info "Second argument to def-fixed-schema must be a positive integer."
              (u/sym-map clj-name size))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)]
    `(def ~clj-name
       (schemas/schema :fixed ~ns-name ~schema-name ~size))))

(defmacro def-flex-map-schema
  [clj-name keys-schema values-schema]
  (when-not keys-schema
    (throw
     (ex-info "Second argument to def-flex-map-schema must be a schema object."
              (u/sym-map clj-name keys-schema))))
  (when-not values-schema
    (throw
     (ex-info "Third argument to def-flex-map-schema must be a schema object."
              (u/sym-map clj-name values-schema))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)]
    `(def ~clj-name
       (schemas/schema :flex-map ~ns-name ~schema-name
                       [~keys-schema ~values-schema]))))
