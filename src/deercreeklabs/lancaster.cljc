(ns deercreeklabs.lancaster
  (:require
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.sub :as sub]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm])
   [schema.core :as s :include-macros true])
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
   with the given field definitions. For a more
   concise way to declare a record schema, see def-record-schema."
  ([fields :- [schemas/RecordFieldDef]]
   (record-schema (keyword (namespace ::x) (str "record-" (hash fields)))
                  nil fields))
  ([name-kw :- s/Keyword
    fields :- [schemas/RecordFieldDef]]
   (record-schema name-kw nil fields))
  ([name-kw :- s/Keyword
    opts :- (s/maybe u/RecordOptionsMap)
    fields :- [schemas/RecordFieldDef]]
   (schemas/schema :record name-kw [opts fields])))

(s/defn enum-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro enum
   with the given symbol keywords. Optionally allows specifying the
   schema name and namespacing options. For a more concise way to declare
   an enum schema, see def-enum-schema."
  ([symbol-keywords :- [s/Keyword]]
   (enum-schema (keyword (namespace ::x) (str "enum-" (hash symbol-keywords)))
                nil symbol-keywords))
  ([name-kw :- s/Keyword
    symbol-keywords :- [s/Keyword]]
   (enum-schema name-kw nil symbol-keywords))
  ([name-kw :- s/Keyword
    opts :- (s/maybe u/EnumOptionsMap)
    symbol-keywords :- [s/Keyword]]
   (schemas/schema :enum name-kw [opts symbol-keywords])))

(s/defn fixed-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro fixed
   with the given size. Optionally allows specifying the schema name.
   For a more concise way to declare a fixed schema, see def-fixed-schema."
  ([size :- s/Int]
   (fixed-schema (keyword (namespace ::x) (str "fixed-" size))
                 size))
  ([name-kw :- s/Keyword
    size :- s/Int]
   (schemas/schema :fixed name-kw size)))

(s/defn array-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro array
   with the given items schema."
  [items-schema :- LancasterSchema]
  (schemas/schema :array nil items-schema))

(s/defn map-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro map
   with the given values schema. Keys are always strings."
  [values-schema :- LancasterSchema]
  (schemas/schema :map nil values-schema))

(s/defn union-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro union
   with the given member schemas."
  [member-schemas :- [LancasterSchemaOrNameKW]]
  (schemas/schema :union nil member-schemas))

(s/defn maybe :- LancasterSchema
  "Creates a Lancaster union schema whose members are l/null-schema
   and the given schema. Makes a schema nillable. If the given schema
   is a union, returns a schema with l/null-schema in the first postion."
  [schema :- LancasterSchemaOrNameKW]
  (if (keyword? schema)
    (union-schema [null-schema schema])
    (let [edn-schema (u/edn-schema schema)]
      (if (not= :union (u/get-avro-type edn-schema))
        (union-schema [null-schema schema])
        (if (= :null (first edn-schema))
          schema
          (schemas/edn-schema->lancaster-schema
           (vec (cons :null edn-schema))))))))

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

(s/defn edn :- s/Any
  "Returns an EDN representation of the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/edn-schema schema))

(s/defn edn->schema :- LancasterSchema
  [edn :- s/Any]
  (schemas/edn-schema->lancaster-schema edn))

(s/defn json :- s/Str
  "Returns an Avro-compliant JSON representation of the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/json-schema schema))

(s/defn name-kw :- s/Keyword
  "Returns the name keyword for the given Lancaster schema."
  [schema :- LancasterSchema]
  (-> (u/edn-schema schema)
      (u/edn-schema->name-kw)))

(s/defn pcf :- s/Str
  "Returns a JSON string containing the Avro Parsing Canonical Form of
  the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/parsing-canonical-form schema))

(s/defn fingerprint64 :- Long
  "Returns the 64-bit Rabin fingerprint of the Parsing Canonical Form
   of the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/fingerprint64 schema))

(s/defn schema? :- s/Bool
  "Returns a boolean indicating whether or not the argument is a
   Lancaster schema object."
  [arg :- s/Any]
  (satisfies? u/ILancasterSchema arg))

(s/defn plumatic-schema :- s/Any
  "Returns a Plumatic schema for the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/plumatic-schema schema))

(s/defn default-data :- s/Any
  "Creates default data that conforms to the given Lancaster schema."
  [schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info "Argument to default-data must be a schema object."
              {:given-arg schema})))
  (u/default-data (edn schema)))

(s/defn sub-schemas :- [LancasterSchema]
  "Returns the unique sub schemas of the given Lancaster schema.
   Includes the given schema."
  [schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info "Argument to sub-schemas must be a schema object."
              {:given-arg schema})))
  (sub/sub-schemas schema))

(s/defn schema-at-path :- LancasterSchema
  [schema :- LancasterSchema
   path :- [s/Any]]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info "First argument to schema-at-path must be a schema object."
              {:given-arg schema})))
  (if (nil? path)
    schema
    (when-not (u/path? path)
      (throw
       (ex-info
        (str "Second argument to schema-at-path must be nil or a sequence of "
             "path keys, which must be keywords, strings, or integers.")
        {:given-path path}))))
  (sub/schema-at-path schema path))

(s/defn schema-type :- s/Keyword
  "Returns the Avro type of the given schema"
  [schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info "Argument to `schema-type` must be a schema object."
              {:given-arg schema})))
  (u/get-avro-type (u/edn-schema schema)))

;;;;;;;;;; Named Schema Helper Macros ;;;;;;;;;;;;;;;;

(defmacro def-record-schema
  "Defines a var whose value is a Lancaster record schema object"
  [clj-name & args]
  (when-not (pos? (count args))
    (throw
     (ex-info "Missing record fields in def-record-schema."
              (u/sym-map clj-name args))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        [opts fields] (if (map? (first args))
                        [(first args) (rest args)]
                        [nil args])]
    `(def ~clj-name
       (schemas/schema :record ~ns-name ~schema-name
                       [~opts (vector ~@fields)]))))

(defmacro def-enum-schema
"Defines a var whose value is a Lancaster enum schema object"
[clj-name & args]
(when-not (pos? (count args))
(throw
(ex-info "Missing symbol-keywords sequence in def-enum-schema."
         (u/sym-map clj-name args))))
(let [ns-name (str (or
                    (:name (:ns &env)) ;; cljs
                    *ns*))             ;; clj
schema-name (u/schema-name clj-name)
[opts symbol-keywords] (if (map? (first args))
                         [(first args) (rest args)]
                         [nil args])]
`(def ~clj-name
(schemas/schema :enum ~ns-name ~schema-name
                [~opts (vector ~@symbol-keywords)]))))

(defmacro def-fixed-schema
"Defines a var whose value is a Lancaster fixed schema object"
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

(defmacro def-array-schema
  "Defines a var whose value is a Lancaster array schema object"
  [clj-name items-schema]
  (when-not items-schema
    (throw
     (ex-info "Second argument to def-array-schema must be a schema object."
              (u/sym-map clj-name items-schema))))
  `(def ~clj-name
     (schemas/schema :array nil ~items-schema)))

(defmacro def-map-schema
  "Defines a var whose value is a Lancaster map schema object"
  [clj-name values-schema]
  (when-not values-schema
    (throw
     (ex-info "Second argument to def-map-schema must be a schema object."
              (u/sym-map clj-name values-schema))))
  `(def ~clj-name
     (schemas/schema :map nil ~values-schema)))

(defmacro def-union-schema
  "Defines a var whose value is a Lancaster union schema object"
  [clj-name & member-schemas]
  `(def ~clj-name
     (schemas/schema :union nil (vector ~@member-schemas))))

(defmacro def-maybe-schema
  "Defines a var whose value is a Lancaster union schema whose members
  are l/null-schema and the given schema. Makes a schema nillable."
  [clj-name schema]
  `(def ~clj-name
     (union-schema [null-schema ~schema])))
