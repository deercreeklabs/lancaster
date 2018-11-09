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

(def ^:no-doc LancasterSchema (s/protocol u/ILancasterSchema))
(def ^:no-doc LancasterSchemaOrNameKW (s/if keyword?
                                        s/Keyword
                                        LancasterSchema))
#?(:cljs
   (def Long ^:no-doc gm/Long))

(s/defn json->schema :- LancasterSchema
  "Creates a Lancaster schema object from an Avro schema in JSON format."
  [json :- s/Str]
  (schemas/json-schema->lancaster-schema json))

(def int-schema
  "Lancaster schema object representing an Avro int."
  (schemas/primitive-schema :int))
(def null-schema
  "Lancaster schema object representing an Avro null."
  (schemas/primitive-schema :null))
(def boolean-schema
  "Lancaster schema object representing an Avro boolean."
  (schemas/primitive-schema :boolean))
(def long-schema
  "Lancaster schema object representing an Avro long."
  (schemas/primitive-schema :long))
(def float-schema
  "Lancaster schema object representing an Avro float."
  (schemas/primitive-schema :float))
(def double-schema
  "Lancaster schema object representing an Avro double."
  (schemas/primitive-schema :double))
(def bytes-schema
  "Lancaster schema object representing an Avro bytes."
  (schemas/primitive-schema :bytes))
(def string-schema
  "Lancaster schema object representing an Avro string."
  (schemas/primitive-schema :string))

(s/defn record-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro record
   with the given name keyword and field definitions. For a more
   concise way to declare a record schema, see def-record-schema."
  [name-kw :- s/Keyword
   fields :- [schemas/RecordFieldDef]]
  (schemas/schema :record name-kw fields))

(s/defn enum-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro enum
   with the given name and symbols. For a more
   concise way to declare an enum schema, see def-enum-schema."
  [name-kw :- s/Keyword
   symbols :- [s/Keyword]]
  (schemas/schema :enum name-kw symbols))

(s/defn fixed-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro fixed
   with the given name and size. For a more
   concise way to declare a fixed schema, see def-fixed-schema."
  [name-kw :- s/Keyword
   size :- s/Int]
  (schemas/schema :fixed name-kw size))

;; TODO: Document this
(s/defn flex-map-schema :- LancasterSchema
  [name-kw :- s/Keyword
   keys-schema :- LancasterSchema
   values-schema :- LancasterSchema]
  (schemas/schema :flex-map name-kw [keys-schema values-schema]))

(s/defn array-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro array
   with the given items schema."
  [items-schema :- LancasterSchema]
  (schemas/schema :array nil items-schema))

(s/defn map-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro map
   with the given values schema."
  [values-schema :- LancasterSchema]
  (schemas/schema :map nil values-schema))

(s/defn union-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro union
   with the given member schemas."
  [member-schemas :- [LancasterSchemaOrNameKW]]
  (schemas/schema :union nil member-schemas))

(s/defn merge-record-schemas :- LancasterSchema
  "Creates a Lancaster record schema which contains all the fields
   of all record schemas passed in."
  [name-kw :- s/Keyword
   schemas :- [LancasterSchema]]
  (schemas/merge-record-schemas name-kw schemas))

(s/defn maybe :- LancasterSchema
  "Creates a Lancaster union schema whose members are l/null-schema
   and the given schema. Makes a schema nillable."
  [schema :- LancasterSchemaOrNameKW]
  (union-schema [null-schema schema]))

(s/defn serialize :- ba/ByteArray
  "Serializes data to a byte array, using the given Lancaster schema."
  [writer-schema :- LancasterSchema
   data :- s/Any]
  (when-not (satisfies? u/ILancasterSchema writer-schema)
    (throw
     (ex-info (str "First argument to serialize must be a schema "
                   "object. The object must satisfy the ILancasterSchema "
                   "protocol.")
              {:schema writer-schema
               :schema-type (#?(:clj class :cljs type) writer-schema)})))
  (u/serialize writer-schema data))

(s/defn deserialize :- s/Any
  "Deserializes Avro-encoded data from a byte array, using the given reader and
   writer schemas."
  [reader-schema :- LancasterSchema
   writer-schema :- LancasterSchema
   ba :- ba/ByteArray]
  (when-not (satisfies? u/ILancasterSchema reader-schema)
    (throw
     (ex-info (str "First argument to deserialize must be a schema "
                   "object representing the reader's schema. The object "
                   "must satisfy the ILancasterSchema protocol.")
              {:reader-schema reader-schema
               :reader-schema-type
               (#?(:clj class :cljs type) reader-schema)})))
  (when-not (satisfies? u/ILancasterSchema writer-schema)
    (throw
     (ex-info (str "Second argument to deserialize must be a schema "
                   "object representing the writer's schema. The object "
                   "must satisfy the ILancasterSchema protocol.")
              {:writer-schema writer-schema
               :writer-schema-type
               (#?(:clj class :cljs type) writer-schema)})))
  (when-not (instance? ba/ByteArray ba)
    (throw (ex-info (str "Final argument to deserialize must be a byte array. "
                         "The byte array must include the binary data to "
                         "be deserialized.")
                    {:ba ba
                     :ba-type (#?(:clj class :cljs type) ba)})))
  (let [is (impl/input-stream ba)]
    (u/deserialize reader-schema writer-schema is)))

(s/defn deserialize-same :- s/Any
  "Deserializes Avro-encoded data from a byte array, using the given schema
   as both the reader and writer schema. Note that this is not recommended,
   since the original writer's schema should always be used to deserialize.
   The writer's schema (in Parsing Canonical Form) should always be stored
   or transmitted with encoded data."
  [schema :- LancasterSchema
   ba :- ba/ByteArray]
  (deserialize schema schema ba))

(s/defn wrap :- schemas/WrappedData
  "Wraps data for use in an ambiguous union."
  [data-schema :- LancasterSchema
   data :- s/Any]
  (u/wrap data-schema data))

(s/defn edn-schema :- s/Any
  [schema :- LancasterSchema]
  (u/edn-schema schema))

(s/defn json :- s/Str
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
