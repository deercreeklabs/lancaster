(ns deercreeklabs.lancaster.schemas
  (:require
   [camel-snake-kebab.core :as csk]
   [cheshire.core :as json]
   #?(:clj [deercreeklabs.lancaster.gen :as gen])
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:clj
     (:import
      (java.io ByteArrayOutputStream)
      (org.apache.avro Schema Schema$Parser SchemaNormalization)
      (org.apache.avro.io BinaryDecoder Decoder DecoderFactory
                          Encoder EncoderFactory)
      (org.apache.avro.specific SpecificDatumReader SpecificDatumWriter))
     :cljs
     (:require-macros
      deercreeklabs.lancaster.utils)))

(defprotocol IAvroSchema
  (serialize [this data])
  (deserialize [this writer-schema ba return-java?])
  (get-edn-schema [this])
  (get-json-schema [this])
  (get-canonical-parsing-form [this])
  (get-fingerprint128 [this]))

(def ^EncoderFactory encoder-factory (EncoderFactory/get))
(def ^DecoderFactory decoder-factory (DecoderFactory/get))

(defn serialize* [^SpecificDatumWriter writer java-data-obj]
  (let [^ByteArrayOutputStream output-stream (ByteArrayOutputStream.)
        ^Encoder encoder (.binaryEncoder encoder-factory output-stream nil)]
    (.write writer java-data-obj encoder)
    (.flush encoder)
    (.toByteArray output-stream)))

(defn deserialize* [^SpecificDatumReader reader ba]
  ;; TODO: Handle differing read/write schemas (ResolvingDecoder)
  ;; TODO: Wrap unions
  (let [^BinaryDecoder decoder (.binaryDecoder decoder-factory ^bytes ba nil)]
    (.read reader nil decoder)))

(defrecord AvroNamedSchema [edn-schema json-schema canonical-parsing-form
                            constructor post-converter writer reader
                            fingerprint128]
  IAvroSchema
  (serialize [this data]
    (serialize* writer (constructor data)))
  (deserialize [this writer-schema ba return-java?]
    (let [obj (deserialize* reader ba)]
      (if return-java?
        obj
        (post-converter obj))))
  (get-edn-schema [this]
    edn-schema)
  (get-json-schema [this]
    json-schema)
  (get-canonical-parsing-form [this]
    canonical-parsing-form)
  (get-fingerprint128 [this]
    fingerprint128))

(def avro-named-types #{:record :fixed :enum})

(defn get-schema-name [edn-schema]
  (cond
    (avro-named-types (:type edn-schema))
    (if-let [schema-ns (:namespace edn-schema)]
      (str schema-ns "." (:name edn-schema))
      (:name edn-schema))

    (map? edn-schema)
    (:type edn-schema)

    (nil? edn-schema)
    (throw (ex-info "Schema is nil."
                    {:type :illegal-argument
                     :subtype :schema-is-nil
                     :schema edn-schema}))

    :else
    edn-schema))

(defn get-avro-type [edn-schema]
  (cond
    (sequential? edn-schema) :union
    (map? edn-schema) (:type edn-schema)
    (nil? edn-schema) (throw (ex-info "Schema is nil."
                                      {:type :illegal-schema
                                       :subtype :schema-is-nil
                                       :schema edn-schema}))
    :else edn-schema))

(defn first-arg-dispatch [first-arg & rest-of-args]
  first-arg)

(defn drop-schema-from-name [s]
  (-> (name s)
      (clojure.string/split #"-schema")
      (first)))

(defn make-default-record [edn-schema]
  (let [add-field (fn [acc {:keys [type name default]}]
                    (let [avro-type (get-avro-type type)
                          val (if (= :record avro-type)
                                (make-default-record type)
                                default)]
                      (assoc acc name val)))]
    (reduce add-field {} (:fields edn-schema))))

(defn make-default-enum [enum-edn-schema field-default]
  (let [sym (or field-default
                (first (:symbols enum-edn-schema)))]
    (-> (name sym)
        (csk/->SCREAMING_SNAKE_CASE))))

(defn get-field-default [field-schema avro-type field-default]
  (if (= :enum avro-type)
    (make-default-enum field-schema field-default)
    (or field-default
        (case avro-type
          :null nil
          :boolean false
          :int (int -1)
          :long -1
          :float (float -1.0)
          :double (double -1.0)
          :bytes ""
          :string ""
          :array []
          :map {}
          :fixed ""
          :union (first field-schema)
          :record (make-default-record field-schema)))))

(defmulti make-edn-schema (fn [avro-type & args]
                            avro-type))

(defmethod make-edn-schema :record
  [schema-type schema-ns class-name fields]
  (let [make-field (fn [[field-name field-schema field-default]]

                     (let [edn-schema (if (satisfies? IAvroSchema field-schema)
                                        (get-edn-schema field-schema)
                                        field-schema)
                           avro-type (get-avro-type edn-schema)]
                       {:name (csk/->camelCase (name field-name))
                        :type (get-schema-name edn-schema)
                        :default (get-field-default edn-schema avro-type
                                                    field-default)}))]
    {:namespace schema-ns
     :name class-name
     :type :record
     :fields (mapv make-field fields)}))

(defmethod make-edn-schema :enum
  [schema-type schema-ns class-name symbols]
  {:namespace schema-ns
   :name class-name
   :type :enum
   :symbols (vec symbols)})

(defmethod make-edn-schema :fixed
  [schema-type schema-ns class-name size]
  {:namespace schema-ns
   :name class-name
   :type :fixed
   :size size})

(defn avro-type-dispatch [edn-schema & args]
  (get-avro-type edn-schema))

(defmulti make-constructor avro-type-dispatch)

(defmethod make-constructor :record
  [edn-schema short-name class-expr constructor-sym]
  (let [{class-name :name
         schema-ns :namespace
         fields :fields} edn-schema
        builder (symbol (str schema-ns "." class-name "/newBuilder"))
        make-setter #(let [field-name (:name %)
                           field-sym (symbol (str field-name "#"))
                           setter (symbol (str ".set"
                                               (csk/->PascalCase field-name)))]
                       `(~field-sym (~setter ~field-sym)))
        setters (-> (mapcat make-setter fields)
                    (concat `(true (.build))))
        map-sym (gensym "map")
        make-arg-destructure #(let [field-name (:name %)
                                    field-sym (symbol (str field-name "#"))
                                    field-kw (keyword field-name)]
                                `(~field-sym (get ~map-sym ~field-kw)))
        destructure-expr (mapcat make-arg-destructure fields)
        p-constructor-sym (symbol (str "->" short-name))
        p-constructor-args (map #(-> % :name (str "#") symbol) fields)]
    `(do
       (import ~class-expr)
       (defn ~p-constructor-sym [~@p-constructor-args]
         (cond-> (~builder)
           ~@setters))
       (defn ~constructor-sym [~map-sym]
         (let [~@destructure-expr]
           (cond-> (~builder)
             ~@setters))))))

(defmethod make-constructor :enum
  [edn-schema short-name class-expr constructor-sym]
  (let [{class-name :name
         schema-ns :namespace
         symbols :symbols} edn-schema
        make-pair (fn [symbol-kw]
                    (let [const-name (csk/->SCREAMING_SNAKE_CASE
                                      (name symbol-kw))
                          java-enum (symbol (str schema-ns "." class-name
                                                 "/" const-name))]
                      `(~symbol-kw ~java-enum)))
        pairs (mapcat make-pair symbols)]
    `(do
       (import ~class-expr)
       (defn ~constructor-sym [enum-kw#]
         (case enum-kw#
           ~@pairs)))))

(defmethod make-constructor :fixed
  [edn-schema short-name class-expr constructor-sym]
  (let [{class-name :name
         schema-ns :namespace} edn-schema
        java-constructor (symbol (str schema-ns "." class-name "."))]
    `(do
       (import ~class-expr)
       (defn ~constructor-sym [ba#]
         (~java-constructor ba#)))))

(defmulti make-post-converter avro-type-dispatch)

(defmethod make-post-converter :record
  [edn-schema fq-class post-convert-sym]
  (let [field-names (map :name (:fields edn-schema))
        avro-obj-sym (symbol "avro-obj")
        make-kv (fn [field-name]
                  (let [kw (keyword field-name)
                        getter (symbol (str ".get"
                                            (csk/->PascalCase field-name)))]
                    `(~kw (~getter ~avro-obj-sym))))
        m (apply hash-map (mapcat make-kv field-names))]
    `(defn ~post-convert-sym [~(with-meta avro-obj-sym {:tag fq-class})]
       ~m)))

(defmethod make-post-converter :enum
  [edn-schema fq-class post-convert-sym]
  (let [{class-name :name
         schema-ns :namespace
         symbols :symbols} edn-schema
        make-pair (fn [symbol-kw]
                    (let [const-name (csk/->SCREAMING_SNAKE_CASE
                                      (name symbol-kw))
                          java-enum (symbol (str schema-ns "." class-name
                                                 "/" const-name))]
                      `(~java-enum ~symbol-kw)))
        pairs (mapcat make-pair (:symbols edn-schema))]
    `(defn ~post-convert-sym [java-enum#]
       (condp = java-enum#
         ~@pairs))))

(defmethod make-post-converter :fixed
  [edn-schema fq-class post-convert-sym]
  (let [avro-obj-sym (symbol "avro-obj")]
    `(defn ~post-convert-sym [~(with-meta avro-obj-sym {:tag fq-class})]
       (.bytes ~avro-obj-sym))))

(defn get-schema-constructor [avro-type]
  (if (avro-named-types avro-type)
    ->AvroNamedSchema
    (throw (ex-info "Bad avro type" {:avro-type avro-type}))))

(defn xf-record-schema [edn-schema]
  (let [xf-enum-default #(-> % name csk/->SCREAMING_SNAKE_CASE)
        xf-field (fn [field]
                   (let [field-schema (:type field)
                         edn-schema (if (satisfies? IAvroSchema field-schema)
                                      (get-edn-schema field-schema)
                                      field-schema)
                         avro-type (get-avro-type edn-schema)]
                     (case avro-type
                       :enum (update field :default xf-enum-default)
                       field)))]
    (update edn-schema :fields #(mapv xf-field %))))

(defn xf-enum-schema [edn-schema]
  (let [xf-symbol #(-> % name csk/->SCREAMING_SNAKE_CASE)]
    (update edn-schema :symbols #(mapv xf-symbol %))))

(defn make-json-schema [edn-schema]
  (let [avro-type (get-avro-type edn-schema)
        xfer (case avro-type
               :record xf-record-schema
               :enum xf-enum-schema
               identity)]
    (json/generate-string (xfer edn-schema) {:pretty true})))

(defmacro named-schema-helper*
  [schema-type clj-var-name args]
  (let [short-name (drop-schema-from-name clj-var-name)
        schema-ns (namespace-munge *ns*)
        class-name (csk/->PascalCase short-name)
        class-expr (map symbol [schema-ns class-name])
        fq-class (symbol (str schema-ns "." class-name))
        edn-schema (make-edn-schema schema-type schema-ns class-name args)
        json-schema (make-json-schema edn-schema)
        schema-name (get-schema-name edn-schema)
        avro-type (get-avro-type edn-schema)
        constructor-sym (gensym (str "clj->" short-name))
        post-convert-sym (gensym (str short-name "->clj"))
        schema-constructor (get-schema-constructor avro-type)
        constructor (make-constructor edn-schema short-name class-expr
                                      constructor-sym)
        post-converter (make-post-converter edn-schema fq-class
                                            post-convert-sym)]
    #?(:clj (gen/generate-class class-name json-schema))
    `(let [avro-schema-obj# (.parse (Schema$Parser.) ^String ~json-schema)
           cpf# (SchemaNormalization/toParsingForm avro-schema-obj#)
           fingerprint# (SchemaNormalization/parsingFingerprint
                         "MD5" avro-schema-obj#)
           writer# (SpecificDatumWriter. ^Schema avro-schema-obj#)
           reader# (SpecificDatumReader. ^Schema avro-schema-obj#)]
       ~constructor
       ~post-converter
       (def ~clj-var-name (~schema-constructor ~edn-schema ~json-schema
                           cpf# ~constructor-sym ~post-convert-sym
                           writer# reader# fingerprint#)))))
