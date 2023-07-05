(ns deercreeklabs.lancaster
  (:require
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.sub :as sub]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm]))
  #?(:cljs
     (:require-macros deercreeklabs.lancaster))
  #?(:cljs
     (:import
      (goog.math Long))))

#?(:clj (set! *warn-on-reflection* true))
(def ^:no-doc valid-add-record-name-values #{:always :never :when-ambiguous})

(defn json->schema
  "Creates a Lancaster schema object from an Avro schema in JSON format."
  [json]
  (when-not (string? json)
    (throw (ex-info (str "Argument to json->schema must be a string. Got "
                         (if (nil? json)
                           "nil."
                           (str "`" json "`.")))
                    (u/sym-map json))))
  (let [edn-schema (-> json
                       (u/json-schema->avro-schema)
                       (u/avro-schema->edn-schema))]
    (schemas/edn-schema->lancaster-schema
     {:*name->serializer (atom {})
      :edn-schema edn-schema
      :json-schema json
      :name->edn-schema (u/make-name->edn-schema edn-schema)})))

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

(defn record-schema
  "Creates a Lancaster schema object representing an Avro record
   with the given field definitions. For a more
   concise way to declare a record schema, see def-record-schema."
  ;; Field def: [field-name [docstring] [:required] field-schema [default]]
  ([name-kw fields]
   (schemas/schema :record name-kw nil fields))
  ([name-kw docstring fields]
   (schemas/schema :record name-kw docstring fields)))

(defn enum-schema
  "Creates a Lancaster schema object representing an Avro enum
   with the given symbol keywords. For a more concise way to declare
   an enum schema, see def-enum-schema."
  ([name-kw symbol-keywords]
   (schemas/schema :enum name-kw nil symbol-keywords))
  ([name-kw docstring symbol-keywords]
   (schemas/schema :enum name-kw docstring symbol-keywords)))

(defn fixed-schema
  "Creates a Lancaster schema object representing an Avro fixed
   with the given size. For a more concise way to declare a fixed
   schema, see def-fixed-schema."
  ([name-kw size]
   (schemas/schema :fixed name-kw size)))

(defn array-schema
  "Creates a Lancaster schema object representing an Avro array
   with the given items schema."
  [items-schema]
  (schemas/schema :array nil items-schema))

(defn map-schema
  "Creates a Lancaster schema object representing an Avro map
   with the given values schema. Keys are always strings."
  [values-schema]
  (schemas/schema :map nil values-schema))

(defn union-schema
  "Creates a Lancaster schema object representing an Avro union
   with the given member schemas."
  [member-schemas]
  (schemas/schema :union nil member-schemas))

(def string-set-schema
  "Lancaster schema object representing a Clojure set with string members.
   Implemented using an Avro map with null values."
  (map-schema null-schema))

(defn maybe
  "Creates a Lancaster union schema whose members are l/null-schema
   and the given schema. Makes a schema nillable. If the given schema
   is a union, returns a schema with l/null-schema in the first postion.
   If the given union schema already has l/null-schema as a member, it
   is returned unchanged. Similarly, if the given schema is
   l/null-schema, it is returned unchanged."
  [schema]
  (schemas/maybe schema))

(defn serialize
  "Serializes data to a byte array, using the given Lancaster schema."
  [writer-schema
   data]
  (when-not (satisfies? u/ILancasterSchema writer-schema)
    (throw
     (ex-info (str "First argument to `serialize` must be a schema "
                   "object. The object must satisfy the ILancasterSchema "
                   "protocol. Got `" writer-schema "`.")
              {:schema (u/pprint-str writer-schema)
               :schema-type (#?(:clj class :cljs type) writer-schema)})))
  (u/serialize writer-schema data))


(defn deserialize
  "Deserializes Avro-encoded data from a byte array, using the given reader and
   writer schemas."
  ([reader-schema writer-schema ba]
   (deserialize reader-schema writer-schema ba {}))
  ([reader-schema writer-schema ba opts]
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
   (let [is (impl/input-stream ba)
         {:keys [add-record-name]} opts
         opts* (cond-> opts
                 (not (contains? opts :add-record-name))
                 (assoc :add-record-name :when-ambiguous))]
     (u/deserialize reader-schema writer-schema is opts*))))

(defn deserialize-same
  "Deserializes Avro-encoded data from a byte array, using the given schema
   as both the reader and writer schema. Note that this is not recommended,
   since the original writer's schema should always be used to deserialize.
   The writer's schema (in Parsing Canonical Form) should always be stored
   or transmitted with encoded data."
  ([schema ba]
   (deserialize schema schema ba {}))
  ([schema ba opts]
   (deserialize schema schema ba opts)))

(defn edn
  "Returns an EDN representation of the given Lancaster schema."
  [schema ]
  (u/edn-schema schema))

(defn edn->schema
  [edn]
  (schemas/edn-schema->lancaster-schema
   {:*name->serializer (atom {})
    :edn-schema edn
    :name->edn-schema (u/make-name->edn-schema edn)}))

(defn json
  "Returns an Avro-compliant JSON representation of the given Lancaster schema."
  [schema]
  (u/json-schema schema))

(defn name-kw
  "Returns the name keyword for the given Lancaster schema."
  [schema]
  (-> (u/edn-schema schema)
      (u/edn-schema->name-kw)))

(defn pcf
  "Returns a JSON string containing the Avro Parsing Canonical Form of
  the given Lancaster schema."
  [schema]
  (u/parsing-canonical-form schema))

(defn fingerprint64
  "Returns the 64-bit Rabin fingerprint of the Parsing Canonical Form
   of the given Lancaster schema."
  [schema]
  (u/fingerprint64 schema))

(defn fingerprint128
  "Returns the 128-bit MD5 digest of the Parsing Canonical Form
   of the given Lancaster schema."
  [schema]
  (u/fingerprint128 schema))

(defn fingerprint256
  "Returns the 256-bit SHA-256 hash of the Parsing Canonical Form
   of the given Lancaster schema."
  [schema]
  (u/fingerprint256 schema))

(defn schema?
  "Returns a boolean indicating whether or not the argument is a
   Lancaster schema object."
  [arg]
  (satisfies? u/ILancasterSchema arg))

(defn schemas-match?
  "Returns a boolean indicating whether or not the given reader and
   writer schemas match, according to the Avro matching rules."
  [reader-schema writer-schema]
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


(defn default-data
  "Creates default data that conforms to the given Lancaster schema."
  [schema]
  (when-not (satisfies? u/ILancasterSchema schema)
    (throw
     (ex-info (str "Argument to default-data must be a schema object. Got `"
                   schema "`.")
              {:given-arg schema})))
  (u/default-data (edn schema)))

(defn child-schema
  "Returns the child schema of the given schema"
  ([schema]
   (u/child-schema schema))
  ([schema field-kw-or-branch-i]
   (u/child-schema schema field-kw-or-branch-i)))

(defn schema-at-path
  ([schema path]
   (schema-at-path schema path {}))
  ([schema path {:keys [branches?] :as opts}]
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
   (when (and (not (nil? branches?)) (not (boolean? branches?)))
     (throw
      (ex-info
       (str "`branches?` option, when provided, must be a boolean. When true "
            "it causes integers in the path to be interpreted as branches in "
            "union if the corresponding schema is a union rather than causing "
            "a search in the union for an array. Got: `" branches? "`.")
       {:given-opts opts})))
   (sub/schema-at-path schema path opts)))

(defn member-schemas
  "Returns the member schemas of the given union schema."
  [union-schema]
  (when-not (satisfies? u/ILancasterSchema union-schema)
    (throw
     (ex-info (str "The argument to `member-schemas` must satisfy the "
                   "ILancasterSchema protocol. Got `"
                   (or union-schema "nil") "`.")
              {:schema (u/pprint-str union-schema)
               :schema-type (#?(:clj class :cljs type) union-schema)})))
  (sub/member-schemas union-schema))

(defn member-schema-at-branch
  "Returns the member schema at the given union schema branch index."
  [union-schema
   branch-index]
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

(defn schema-type
  "Returns the Avro type of the given schema"
  [schema]
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
