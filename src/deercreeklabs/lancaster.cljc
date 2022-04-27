(ns deercreeklabs.lancaster
  (:require
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.bilt :as bilt]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.sub :as sub]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm])
   [schema.core :as s :include-macros true])
  #?(:cljs
     (:require-macros deercreeklabs.lancaster))
  #?(:cljs
     (:import
      (goog.math Long))))

(def ^:no-doc LancasterSchema (s/protocol u/ILancasterSchema))
(def ^:no-doc LancasterSchemaOrNameKW (s/if keyword?
                                        s/Keyword
                                        LancasterSchema))

(s/defn json->schema :- LancasterSchema
  "Creates a Lancaster schema object from an Avro schema in JSON format."
  [json :- s/Str]
  (when-not (string? json)
    (throw (ex-info (str "Argument to json->schema must be a string. Got "
                         (if (nil? json)
                           "nil."
                           (str "`" json "`.")))
                    (u/sym-map json))))
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
  ;; Field def: [field-name [docstring] [:required] field-schema [default]]
  ([name-kw :- s/Keyword
    fields :- s/Any]
   (schemas/schema :record name-kw nil fields))
  ([name-kw :- s/Keyword
    docstring :- s/Str
    fields :- s/Any]
   (schemas/schema :record name-kw docstring fields)))

(s/defn enum-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro enum
   with the given symbol keywords. For a more concise way to declare
   an enum schema, see def-enum-schema."
  ([name-kw :- s/Keyword
    symbol-keywords :- [s/Keyword]]
   (schemas/schema :enum name-kw nil symbol-keywords))
  ([name-kw :- s/Keyword
    docstring :- s/Str
    symbol-keywords :- [s/Keyword]]
   (schemas/schema :enum name-kw docstring symbol-keywords)))

(s/defn fixed-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro fixed
   with the given size. For a more concise way to declare a fixed
   schema, see def-fixed-schema."
  ([name-kw :- s/Keyword
    size :- s/Int]
   (schemas/schema :fixed name-kw size)))

(s/defn array-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro array
   with the given items schema."
  [items-schema :- LancasterSchemaOrNameKW]
  (schemas/schema :array nil items-schema))

(s/defn map-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro map
   with the given values schema. Keys are always strings."
  [values-schema :- LancasterSchemaOrNameKW]
  (schemas/schema :map nil values-schema))

(s/defn union-schema :- LancasterSchema
  "Creates a Lancaster schema object representing an Avro union
   with the given member schemas."
  [member-schemas :- [LancasterSchemaOrNameKW]]
  (schemas/schema :union nil member-schemas))

(def string-set-schema
  "Lancaster schema object representing a Clojure set with string members.
   Implemented using an Avro map with null values."
  (map-schema null-schema))

(s/defn maybe :- LancasterSchema
  "Creates a Lancaster union schema whose members are l/null-schema
   and the given schema. Makes a schema nillable. If the given schema
   is a union, returns a schema with l/null-schema in the first postion.
   If the given union schema already has l/null-schema as a member, it
   is returned unchanged. Similarly, if the given schema is
   l/null-schema, it is returned unchanged."
  [schema :- LancasterSchemaOrNameKW]
  (schemas/maybe schema))

(s/defn serialize :- ba/ByteArray
  "Serializes data to a byte array, using the given Lancaster schema."
  [writer-schema :- LancasterSchema
   data :- s/Any]
  (when-not (satisfies? u/ILancasterSchema writer-schema)
    (throw
     (ex-info (str "First argument to `serialize` must be a schema "
                   "object. The object must satisfy the ILancasterSchema "
                   "protocol. Got `" writer-schema "`.")
              {:schema (u/pprint-str writer-schema)
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
     (ex-info (str "First argument to `deserialize` must be a schema "
                   "object representing the reader's schema. The object "
                   "must satisfy the ILancasterSchema protocol. Got: `"
                   reader-schema "`.")
              {:reader-schema (u/pprint-str reader-schema)
               :reader-schema-type
               (#?(:clj class :cljs type) reader-schema)})))
  (when-not (satisfies? u/ILancasterSchema writer-schema)
    (throw
     (ex-info (str "Second argument to `deserialize` must be a schema "
                   "object representing the writer's schema. The object "
                   "must satisfy the ILancasterSchema protocol. Got `"
                   writer-schema "`.")
              {:writer-schema (u/pprint-str writer-schema)
               :writer-schema-type
               (#?(:clj class :cljs type) writer-schema)})))
  (when-not (instance? ba/ByteArray ba)
    (throw (ex-info (str "Final argument to `deserialize` must be a byte "
                         "array. The byte array must include the binary data "
                         "to be deserialized. Got `" ba "`.")
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

(s/defn fingerprint128 :- ba/ByteArray
  "Returns the 128-bit MD5 digest of the Parsing Canonical Form
   of the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/fingerprint128 schema))

(s/defn fingerprint256 :- ba/ByteArray
  "Returns the 256-bit SHA-256 hash of the Parsing Canonical Form
   of the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/fingerprint256 schema))

(s/defn schema? :- s/Bool
  "Returns a boolean indicating whether or not the argument is a
   Lancaster schema object."
  [arg :- s/Any]
  (satisfies? u/ILancasterSchema arg))

(s/defn schemas-match? :- s/Bool
  "Returns a boolean indicating whether or not the given reader and
   writer schemas match, according to the Avro matching rules."
  [reader-schema :- LancasterSchema
   writer-schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema reader-schema)
    (throw
     (ex-info (str "reader-schema must be a schema object. Got `"
                   reader-schema "`.")
              {:given-arg reader-schema})))
  (when-not (satisfies? u/ILancasterSchema writer-schema)
    (throw
     (ex-info (str "writer-schema must be a schema object. Got `"
                   writer-schema "`.")
              {:given-arg writer-schema})))
  (schemas/match? reader-schema writer-schema))

(s/defn plumatic-schema :- s/Any
  "Returns a Plumatic schema for the given Lancaster schema."
  [schema :- LancasterSchema]
  (u/plumatic-schema schema))

(s/defn default-data :- s/Any
  "Creates default data that conforms to the given Lancaster schema."
  [schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info (str "Argument to default-data must be a schema object. Got `"
                   schema "`.")
              {:given-arg schema})))
  (u/default-data (edn schema)))

(s/defn child-schema :- LancasterSchema
  "Returns the child schema of the given schema"
  ([schema]
   (u/child-schema schema))
  ([schema field-kw-or-branch-i]
   (u/child-schema schema field-kw-or-branch-i)))

(s/defn schema-at-path :- (s/maybe LancasterSchema)
  [schema :- LancasterSchema
   path :- [s/Any]]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info (str "First argument to schema-at-path must be a schema object. "
                   "Got `" schema "`.")
              {:given-arg schema})))
  (if (nil? path)
    schema
    (when-not (u/path? path)
      (throw
       (ex-info
        (str "Second argument to schema-at-path must be nil or a sequence of "
             "path keys, which must be keywords, strings, or integers. Got: `"
             path "`.")
        {:given-path path}))))
  (sub/schema-at-path schema path))

(s/defn member-schemas :- [LancasterSchema]
  "Returns the member schemas of the given union schema."
  [union-schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema union-schema)
    (throw
     (ex-info (str "The argument to `member-schemas` must satisfy the "
                   "ILancasterSchema protocol. Got `"
                   (or union-schema "nil") "`.")
              {:schema (u/pprint-str union-schema)
               :schema-type (#?(:clj class :cljs type) union-schema)})))
  (sub/member-schemas union-schema))

(s/defn member-schema-at-branch :- LancasterSchema
  "Returns the member schema at the given union schema."
  [union-schema :- LancasterSchema
   branch-index :- s/Int]
  (when-not (satisfies? u/ILancasterSchema union-schema)
    (throw
     (ex-info (str "The first argument to `member-schema-at-branch` must "
                   "satisfy the  ILancasterSchema protocol. Got `"
                   (or union-schema "nil") "`.")
              {:schema (u/pprint-str union-schema)
               :schema-type (#?(:clj class :cljs type) union-schema)})))
  (when-not (int? branch-index)
    (throw (ex-info
            (str "The second argument to `member-schema-at-branch` must "
                 "be an integer indicating the union branch index. Got `"
                 (or branch-index "nil") "`.")
            (u/sym-map branch-index))))
  (sub/member-schema-at-branch union-schema branch-index))

(s/defn schema-type :- s/Keyword
  "Returns the Avro type of the given schema"
  [schema :- LancasterSchema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info (str "Argument to `schema-type` must be a schema object. Got `"
                   schema "`.")
              {:given-arg schema})))
  (u/get-avro-type (u/edn-schema schema)))

;;;;;;;;;; Named Schema Helper Macros ;;;;;;;;;;;;;;;;

(defmacro def-record-schema
  "Defines a var whose value is a Lancaster record schema object"
  ;; Field def: [field-name [docstring] [:required] field-schema [default]]
  [clj-name & args]
  (when-not (pos? (count args))
    (throw
     (ex-info "Missing record fields in def-record-schema."
              (u/sym-map clj-name args))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)
        [docstring fields] (if (string? (first args))
                             [(first args) (rest args)]
                             [nil args])]
    `(def ~clj-name
       (schemas/schema :record ~name-kw ~docstring (vector ~@fields)))))

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
        name-kw (keyword ns-name schema-name)
        [docstring symbols] (if (string? (first args))
                              [(first args) (rest args)]
                              [nil args])]
    `(def ~clj-name
       (schemas/schema :enum ~name-kw ~docstring (vector ~@symbols)))))

(defmacro def-fixed-schema
  "Defines a var whose value is a Lancaster fixed schema object"
  [clj-name size]
  (when-not (and (pos? size) (integer? size))
    (throw
     (ex-info (str "Second argument (size )to def-fixed-schema must be a "
                   "positive integer. Got: `" size "`.")
              (u/sym-map clj-name size))))
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (schemas/schema :fixed ~name-kw ~size))))

(defmacro def-array-schema
  "Defines a var whose value is a Lancaster array schema object"
  [clj-name items-schema]
  (when-not items-schema ; This is a symbol not a value at this point
    (throw
     (ex-info (str "When calling `def-array-schema`, `items-schema` argument "
                   "must be a schema object. Got nil.")
              {:clj-name clj-name
               :items-schema (u/pprint-str items-schema)})))
  `(def ~clj-name
     (schemas/schema :array nil ~items-schema)))

(defmacro def-map-schema
  "Defines a var whose value is a Lancaster map schema object"
  [clj-name values-schema]
  (when-not values-schema ; This is a symbol not a value at this point
    (throw
     (ex-info (str "When calling `def-map-schema`, `values-schema` argument "
                   "must be a schema object. Got nil.")
              {:clj-name clj-name
               :values-schema (u/pprint-str values-schema)})))
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

;;;;;;;;;;;;;;;;;;;; Built-in Logical Types ;;;;;;;;;;;;;;;;;;;;

;; TODO: Validate args

(defn int-map-schema
  "Creates a Lancaster schema object representing a map of `int` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw values-schema]
  (bilt/flex-map-schema name-kw "int-map" int-schema values-schema int?))

(defn long-map-schema
  "Creates a Lancaster schema object representing a map of `long` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw values-schema]
  (bilt/flex-map-schema name-kw "long-map" long-schema values-schema
                        u/long-or-int?))

(defn fixed-map-schema
  "Creates a Lancaster schema object representing a map of `long` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw key-size values-schema]
  (when-not (nat-int? key-size)
    (throw (ex-info (str "Second argument to fixed-map-schema must be a "
                         "positive integer. Got `" key-size "`.")
                    (u/sym-map key-size))))
  (let [key-schema-name (keyword (namespace name-kw)
                                 (str (name name-kw) "-value"))
        key-schema (fixed-schema key-schema-name key-size)]
    (bilt/flex-map-schema name-kw "fixed-map" key-schema
                          values-schema ba/byte-array?)))

(defmacro def-int-map-schema
  "Defines a var whose value is an int-map-schema object"
  [clj-name values-schema]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (int-map-schema ~name-kw ~values-schema))))

(defmacro def-long-map-schema
  "Defines a var whose value is an long-map-schema object"
  [clj-name values-schema]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (long-map-schema ~name-kw ~values-schema))))

(defmacro def-fixed-map-schema
  "Defines a var whose value is an fixed-map-schema object"
  [clj-name key-size values-schema]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (fixed-map-schema ~name-kw ~key-size ~values-schema))))

(def keyword-schema
  "Lancaster schema object representing a (possibly namespaced)
   Clojure keyword."
  bilt/keyword-schema)
