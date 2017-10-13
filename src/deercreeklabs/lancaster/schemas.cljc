(ns deercreeklabs.lancaster.schemas
  (:require
   [camel-snake-kebab.core :as csk]
   [cheshire.core :as json]
   [deercreeklabs.baracus :as ba]
   #?(:clj [deercreeklabs.lancaster.gen :as gen])
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.stockroom :as sr]
   #?(:clj [puget.printer :refer [cprint]])
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:clj
     (:import
      (java.io ByteArrayOutputStream)
      (java.nio ByteBuffer)
      (org.apache.avro Schema Schema$Parser SchemaNormalization)
      (org.apache.avro.io BinaryDecoder Decoder DecoderFactory
                          Encoder EncoderFactory ResolvingDecoder)
      (org.apache.avro.specific SpecificDatumReader SpecificDatumWriter))
     :cljs
     (:require-macros
      deercreeklabs.lancaster.utils)))

(declare get-field-default)

(def reader-cache-size 50)
(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-named-types   #{:record :fixed :enum})
(def avro-complex-types #{:record :fixed :enum :array :map :union})

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
    (string? edn-schema) :string-reference
    :else edn-schema))

(defn first-arg-dispatch [first-arg & rest-of-args]
  first-arg)

(defn avro-type-dispatch [edn-schema & args]
  (get-avro-type edn-schema))

(defmulti clj->avro first-arg-dispatch)
(defmulti avro->clj first-arg-dispatch)
(defmulti fix-default avro-type-dispatch)
(defmulti edn-schema->avro-schema avro-type-dispatch)

(defmethod clj->avro :null
  [dispatch-name x]
  nil)

(defmethod clj->avro :boolean
  [dispatch-name x]
  x)

(defmethod clj->avro :int
  [dispatch-name n]
  (int n))

(defmethod clj->avro :long
  [dispatch-name x]
  (long x))

(defmethod clj->avro :double
  [dispatch-name x]
  (double x))

(defmethod clj->avro :float
  [dispatch-name n]
  (float n))

(defmethod clj->avro :string
  [dispatch-name x]
  x)

(defmethod clj->avro :bytes
  [dispatch-name ba]
  (ByteBuffer/wrap ba))

(defmethod avro->clj :null
  [dispatch-name x]
  nil)

(defmethod avro->clj :boolean
  [dispatch-name x]
  x)

(defmethod avro->clj :int
  [dispatch-name x]
  x)

(defmethod avro->clj :long
  [dispatch-name x]
  x)

(defmethod avro->clj :float
  [dispatch-name x]
  x)

(defmethod avro->clj :double
  [dispatch-name x]
  x)

(defmethod avro->clj :bytes
  [dispatch-name ^ByteBuffer bb]
  (.array bb))

(defmethod avro->clj :string
  [dispatch-name ^org.apache.avro.util.Utf8 s]
  (.toString s))

(defprotocol IAvroSchema
  (serialize [this data])
  (deserialize [this writer-json-schema ba return-java?])
  (wrap [this data])
  (get-edn-schema [this])
  (get-json-schema [this])
  (get-parsing-canonical-form [this])
  (get-fingerprint128 [this]))

(def ^EncoderFactory encoder-factory (EncoderFactory/get))
(def ^DecoderFactory decoder-factory (DecoderFactory/get))

(defn serialize* [^SpecificDatumWriter writer java-data-obj]
  (let [^ByteArrayOutputStream output-stream (ByteArrayOutputStream.)
        ^Encoder encoder (.binaryEncoder encoder-factory output-stream nil)]
    (.write writer java-data-obj encoder)
    (.flush encoder)
    (.toByteArray output-stream)))

(defn get-resolving-reader [reader-schema-obj writer-json-schema reader-cache]
  (or (sr/get reader-cache writer-json-schema)
      (let [writer-schema-obj (.parse ^Schema$Parser (Schema$Parser.)
                                      ^String writer-json-schema)
            resolving-reader (SpecificDatumReader. writer-schema-obj
                                                   reader-schema-obj)]
        (sr/put reader-cache writer-json-schema resolving-reader)
        resolving-reader)))

(defn deserialize-resolving*
  [reader-schema-obj writer-json-schema reader-cache ba]
  (let [^BinaryDecoder decoder (.binaryDecoder decoder-factory ^bytes ba nil)
        resolving-reader (get-resolving-reader
                          reader-schema-obj writer-json-schema reader-cache)]
    (.read resolving-reader nil decoder)))

(defn deserialize* [^SpecificDatumReader reader ba]
  (let [^BinaryDecoder decoder (.binaryDecoder decoder-factory ^bytes ba nil)]
    (.read reader nil decoder)))

(defrecord AvroSchema [dispatch-name edn-schema json-schema avro-schema-obj
                       parsing-canonical-form writer reader fingerprint128
                       reader-cache]
  IAvroSchema
  (serialize [this data]
    (let [avro (clj->avro dispatch-name data)]
      (serialize* writer avro)))
  (deserialize [this writer-json-schema ba return-java?]
    (let [deser-fn (if (= writer-json-schema json-schema)
                     deserialize*
                     deserialize-resolving*)
          obj (if (= writer-json-schema json-schema)
                (deserialize* reader ba)
                (deserialize-resolving* avro-schema-obj writer-json-schema
                                        reader-cache ba))]
      (if return-java?
        obj
        (avro->clj dispatch-name obj))))
  (wrap [this data]
    {dispatch-name data})
  (get-edn-schema [this]
    edn-schema)
  (get-json-schema [this]
    json-schema)
  (get-parsing-canonical-form [this]
    parsing-canonical-form)
  (get-fingerprint128 [this]
    fingerprint128))

(defn make-primitive-schema [edn-schema]
  (let [dispatch-name edn-schema
        json-schema (json/generate-string edn-schema)
        parsing-canonical-form json-schema
        avro-schema-obj (.parse (Schema$Parser.) ^String json-schema)
        writer (SpecificDatumWriter. ^Schema avro-schema-obj)
        reader (SpecificDatumReader. ^Schema avro-schema-obj)
        fingerprint128 (SchemaNormalization/parsingFingerprint
                        "MD5" avro-schema-obj)
        reader-cache (sr/make-stockroom reader-cache-size)]
    (->AvroSchema dispatch-name edn-schema json-schema avro-schema-obj
                  parsing-canonical-form writer reader fingerprint128
                  reader-cache)))

(defn ensure-edn-schema [schema]
  (cond
    (satisfies? IAvroSchema schema) (get-edn-schema schema)
    (string? schema) (str *ns* "." schema)
    (sequential? schema) (mapv ensure-edn-schema schema) ;; union
    :else schema))

(defn drop-schema-from-name [s]
  (if-not (clojure.string/ends-with? s "schema")
    s
    (-> (name s)
        (clojure.string/split #"-schema")
        (first))))

(defn edn-schema->dispatch-name [edn-schema]
  (let [avro-type (get-avro-type edn-schema)]
    (cond
      (avro-primitive-types avro-type)
      (get-schema-name edn-schema)

      (avro-named-types avro-type)
      (str (:namespace edn-schema) "." (name (:name edn-schema)))

      (= :union avro-type)
      (str "union-of-" (clojure.string/join
                        "," (map #(name (edn-schema->dispatch-name %))
                                 edn-schema)))

      (= :map avro-type)
      (str "map-of-" (name (edn-schema->dispatch-name
                            (:values edn-schema))))

      (= :array avro-type)
      (str "array-of-" (name (edn-schema->dispatch-name
                              (:items edn-schema))))
      (string? edn-schema)
      edn-schema

      :else
      (throw (ex-info (str "Could not find dispatch name for " edn-schema)
                      {:edn-schema edn-schema})))))

(defn make-default-record [record-edn-schema default-record]
  (reduce (fn [acc field]
            (let [{field-name :name
                   field-type :type
                   field-default :default} field
                  field-schema (ensure-edn-schema field-type)
                  v (get-field-default field-schema
                                       (field-name default-record))]
              (assoc acc field-name v)))
          {} (:fields record-edn-schema)))

(defn make-default-fixed-or-bytes [default]
  (u/byte-array->byte-str (or default (ba/byte-array []))))

(defn get-field-default [field-schema field-default]
  (let [avro-type (get-avro-type field-schema)]
    (case avro-type
      :record (make-default-record field-schema field-default)
      :union (get-field-default (first field-schema) field-default)
      :fixed (make-default-fixed-or-bytes field-default)
      :bytes (make-default-fixed-or-bytes field-default)
      (or field-default
          (case avro-type
            :null nil
            :boolean false
            :int (int -1)
            :long -1
            :float (float -1.0)
            :double (double -1.0)
            :string ""
            :enum (first (:symbols field-schema))
            :array []
            :map {})))))

(defmulti make-edn-schema first-arg-dispatch)

(defn get-name-or-schema [edn-schema *names]
  (let [schema-name (get-schema-name edn-schema)]
    (if (@*names schema-name)
      schema-name
      (do
        (swap! *names conj schema-name)
        edn-schema))))

(defn fix-repeated-schemas
  ([edn-schema]
   (fix-repeated-schemas edn-schema (atom #{})))
  ([edn-schema *names]
   (let [fix-child-names (fn [children]
                           (mapv #(fix-repeated-schemas % *names) children))]
     (case (get-avro-type edn-schema)
       :enum (get-name-or-schema edn-schema *names)
       :fixed (get-name-or-schema edn-schema *names)
       :array (update edn-schema :items fix-child-names)
       :map (update edn-schema :values fix-child-names)
       :union (fix-child-names edn-schema)
       :record (let [name-or-schema (get-name-or-schema edn-schema *names)
                     fix-field (fn [field]
                                 (update field :type
                                         #(fix-repeated-schemas % *names)))]
                 (if (map? name-or-schema)
                   (update edn-schema :fields #(mapv fix-field %))
                   name-or-schema))
       edn-schema))))

(defmethod make-edn-schema :record
  [schema-type schema-ns short-name fields]
  (let [mk-field (fn [[field-name field-type field-default]]
                   (let [field-schema (ensure-edn-schema field-type)]
                     {:name (keyword field-name)
                      :type field-schema
                      :default (get-field-default field-schema
                                                  field-default)}))]
    {:namespace schema-ns
     :name (keyword short-name)
     :type :record
     :fields (mapv mk-field fields)}))

(defmethod make-edn-schema :enum
  [schema-type schema-ns short-name symbols]
  {:namespace schema-ns
   :name (keyword short-name)
   :type :enum
   :symbols symbols})

(defmethod make-edn-schema :fixed
  [schema-type schema-ns short-name size]
  {:namespace schema-ns
   :name (keyword short-name)
   :type :fixed
   :size size})

(defmethod make-edn-schema :array
  [schema-type schema-ns short-name items]
  {:type :array
   :items (ensure-edn-schema items)})

(defmethod make-edn-schema :map
  [schema-type schema-ns short-name values]
  {:type :map
   :values (ensure-edn-schema values)})

(defmethod make-edn-schema :union
  [schema-type _ _ member-schemas]
  (let [schemas (mapv ensure-edn-schema member-schemas)
        types (set (map get-avro-type schemas))]
    (when-not (= (count schemas) (count types))
      (throw (ex-info "Unions may not have members with identical types."
                      {:type :illegal-argument
                       :subtype :duplicate-types-in-union
                       :schemas schemas
                       :types types})))
    schemas))

(defn need-pre-conversion? [avro-type]
  ((conj avro-complex-types :bytes :int :float) avro-type))

(defmulti make-constructor avro-type-dispatch)

(defmethod make-constructor :record
  [edn-schema full-java-name dispatch-name]
  (let [{:keys [fields]} edn-schema
        builder (symbol (str full-java-name "/newBuilder"))
        map-sym (gensym "map")
        mk-setter #(let [field-schema (:type %)
                         avro-type (get-avro-type field-schema)
                         name-kw (:name %)
                         field-dispatch-name (edn-schema->dispatch-name
                                              field-schema)
                         setter (symbol (str ".set"
                                             (csk/->PascalCase (name name-kw))))
                         getter (if (need-pre-conversion? avro-type)
                                  `(clj->avro ~field-dispatch-name
                                              (~name-kw ~map-sym))
                                  `(~name-kw ~map-sym))]
                     `((~name-kw ~map-sym) (~setter ~getter)))
        setters (-> (mapcat mk-setter fields)
                    (concat `(true (.build))))]
    `(defmethod clj->avro ~dispatch-name
       [_# ~map-sym]
       (cond-> (~builder)
         ~@setters))))

(defmethod make-constructor :enum
  [edn-schema full-java-name dispatch-name]
  (let [make-pair (fn [symbol-kw]
                    (let [symbol-str (csk/->SCREAMING_SNAKE_CASE
                                      (name symbol-kw))
                          java-enum (symbol (str full-java-name "/"
                                                 symbol-str))]
                      `(~symbol-kw ~java-enum)))
        pairs (mapcat make-pair (:symbols edn-schema))]
    `(defmethod clj->avro ~dispatch-name
       [_# enum-kw#]
       (case enum-kw#
         ~@pairs))))

(defmethod make-constructor :fixed
  [edn-schema full-java-name dispatch-name]
  (let [java-constructor (symbol (str full-java-name "."))]
    `(defmethod clj->avro ~dispatch-name
       [_# ba#]
       (~java-constructor ba#))))

(defmethod make-constructor :array
  [edn-schema _ dispatch-name]
  (let [{:keys [items]} edn-schema
        items-dispatch-name (edn-schema->dispatch-name items)
        items-avro-type (get-avro-type items)]
    (if (need-pre-conversion? items-avro-type)
      `(defmethod clj->avro ~dispatch-name
         [dispatch-name# arr#]
         (mapv #(clj->avro ~items-dispatch-name %) arr#))
      `(defmethod clj->avro ~dispatch-name
         [dispatch-name# arr#]
         arr#))))

(defmethod make-constructor :map
  [edn-schema _ dispatch-name]
  (let [{:keys [values]} edn-schema
        values-dispatch-name (edn-schema->dispatch-name values)
        values-avro-type (get-avro-type values)]
    (if (need-pre-conversion? values-avro-type)
      `(defmethod clj->avro ~dispatch-name
         [dispatch-name# m#]
         (reduce-kv (fn [acc# k# v#]
                      (assoc acc# k# (clj->avro ~values-dispatch-name v#)))
                    {} m#))
      `(defmethod clj->avro ~dispatch-name
         [dispatch-name# m#]
         m#))))

(defn more-than-one? [set schemas]
  (> (count (keep #(set (get-avro-type %)) schemas)) 1))

(defn wrapping-required? [schemas]
  (or (more-than-one? #{:map :record} schemas)
      (more-than-one? #{:int :long :float :double} schemas)
      (more-than-one? #{:null} schemas)
      (more-than-one? #{:boolean} schemas)
      (more-than-one? #{:string :enum} schemas)
      (more-than-one? #{:bytes :fixed} schemas)
      (more-than-one? #{:array} schemas)))


(defn make-data->dispatch-cond-line [data-sym member-schema]
  ;; Note that we only care about putting things in the right
  ;; category, as this is only used by unwrapped unions
  (let [dispatch-name (edn-schema->dispatch-name member-schema)]
    (case (get-avro-type member-schema)
      :null `((nil? ~data-sym) ~dispatch-name)
      :boolean `((instance? Boolean ~data-sym) ~dispatch-name)
      :int `((integer? ~data-sym) ~dispatch-name)
      :long `((integer? ~data-sym) ~dispatch-name)
      :float `((float? ~data-sym) ~dispatch-name)
      :double `((float? ~data-sym) ~dispatch-name)
      :bytes `((ba/byte-array? ~data-sym) ~dispatch-name)
      :string `((string? ~data-sym) ~dispatch-name)
      :record `((map? ~data-sym) ~dispatch-name)
      :fixed `((ba/byte-array? ~data-sym) ~dispatch-name)
      :enum `((string? ~data-sym) ~dispatch-name)
      :array `((sequential? ~data-sym) ~dispatch-name)
      :map `((map? ~data-sym) ~dispatch-name)
      ;; Since unions can't contain other unions, we don't
      ;; have a case for :union
      )))

(defmethod make-constructor :union
  [edn-schema _ dispatch-name]
  (if (wrapping-required? edn-schema)
    `(defmethod clj->avro ~dispatch-name
       [dispatch-name# wrapped-data#]
       (when-not (nil? wrapped-data#)
         (try
           (apply clj->avro (first wrapped-data#))
           (catch java.lang.IllegalArgumentException e#
             (throw
              (ex-info "This union requires wrapping, but data is not wrapped."
                       {:type :illegal-argument
                        :subtype :union-data-not-wrapped
                        :dispatch-name dispatch-name#
                        :data wrapped-data#}))))))
    (let [data-sym (gensym "data")
          cond-lines (mapcat (partial make-data->dispatch-cond-line data-sym)
                             edn-schema)]
      `(defmethod clj->avro ~dispatch-name
         [dispatch-name# ~data-sym]
         (when-not (nil? ~data-sym)
           (clj->avro (cond ~@cond-lines) ~data-sym))))))

(defn need-post-conversion? [avro-type]
  ((conj avro-complex-types :bytes :string) avro-type))

(defmulti make-post-converter avro-type-dispatch)

(defmethod make-post-converter :record
  [edn-schema full-java-name dispatch-name]
  (let [avro-obj-sym (symbol "avro-obj")
        make-kv (fn [field]
                  (let [field-schema (:type field)
                        avro-type (get-avro-type field-schema)
                        dispatch-name (edn-schema->dispatch-name field-schema)
                        name-kw (:name field)
                        getter (->> name-kw
                                    (name)
                                    (csk/->PascalCase)
                                    (str ".get")
                                    (symbol))
                        get-expr (if (need-post-conversion? avro-type)
                                   `(avro->clj ~dispatch-name
                                               (~getter ~avro-obj-sym))
                                   `(~getter ~avro-obj-sym))
                        class-nk (class name-kw)
                        class-ge (class get-expr)]
                    `(~name-kw ~get-expr)))
        m (apply hash-map (mapcat make-kv (:fields edn-schema)))]
    `(defmethod avro->clj ~dispatch-name
       [dispatch-name# ~(with-meta avro-obj-sym {:tag (symbol full-java-name)})]
       ~m)))

(defmethod make-post-converter :enum
  [edn-schema full-java-name dispatch-name]
  (let [make-pair (fn [symbol-kw]
                    (let [symbol-str (csk/->SCREAMING_SNAKE_CASE
                                      (name symbol-kw))
                          java-enum (symbol (str full-java-name "/"
                                                 symbol-str))]
                      `(~java-enum ~symbol-kw)))
        pairs (mapcat make-pair (:symbols edn-schema))]
    `(defmethod avro->clj ~dispatch-name
       [dispatch-name# java-enum#]
       (condp = java-enum#
         ~@pairs))))

(defmethod make-post-converter :fixed
  [edn-schema full-java-name dispatch-name]
  (let [avro-obj-sym (symbol "avro-obj")]
    `(defmethod avro->clj ~dispatch-name
       [dispatch-name# ~(with-meta avro-obj-sym {:tag (symbol full-java-name)})]
       (.bytes ~avro-obj-sym))))

(defmethod make-post-converter :array
  [edn-schema _ dispatch-name]
  (let [{:keys [items]} edn-schema
        items-dispatch-name (edn-schema->dispatch-name items)
        items-avro-type (get-avro-type items)]
    (if (need-post-conversion? items-avro-type)
      `(defmethod avro->clj ~dispatch-name
         [dispatch-name# arr#]
         (mapv #(avro->clj ~items-dispatch-name %) arr#))
      `(defmethod avro->clj ~dispatch-name
         [dispatch-name# arr#]
         arr#))))

(defmethod make-post-converter :map
  [edn-schema _ dispatch-name]
  (let [{:keys [values]} edn-schema
        values-dispatch-name (edn-schema->dispatch-name values)
        values-avro-type (get-avro-type values)]
    (if (need-post-conversion? values-avro-type)
      `(defmethod avro->clj ~dispatch-name
         [dispatch-name# m#]
         (reduce (fn [acc# [^org.apache.avro.util.Utf8 k# v#]]
                   (assoc acc# (.toString k#)
                          (avro->clj ~values-dispatch-name v#)))
                 {} m#))
      `(defmethod avro->clj ~dispatch-name
         [dispatch-name# m#]
         (reduce (fn [acc# [^org.apache.avro.util.Utf8 k# v#]]
                   (assoc acc# (.toString k#) v#))
                 {} m#)))))

(defn named-schema->java-class [edn-schema]
  (let [java-ns (namespace-munge (:namespace edn-schema))
        java-class-name (csk/->PascalCase (name (:name edn-schema)))]
    (Class/forName (str java-ns "." java-class-name))))

#?(:clj
   (defn edn-schema->java-class [edn-schema]
     (case (get-avro-type edn-schema)
       :null nil
       :boolean java.lang.Boolean
       :int java.lang.Integer
       :long java.lang.Long
       :float java.lang.Float
       :double java.lang.Double
       :bytes java.nio.HeapByteBuffer
       :string org.apache.avro.util.Utf8
       :array org.apache.avro.generic.GenericData$Array
       :map java.util.HashMap
       (named-schema->java-class edn-schema))))

(defn make-class->name [union-schema]
  (let [java-classes (map edn-schema->java-class union-schema)
        dispatch-names (map edn-schema->dispatch-name union-schema)]
    (zipmap java-classes dispatch-names)))

(defmethod make-post-converter :union
  [edn-schema _ dispatch-name]
  (let [class->name (make-class->name edn-schema)]
    (if (wrapping-required? edn-schema)
      `(defmethod avro->clj ~dispatch-name
         [union-dispatch-name# avro-data#]
         (when-not (nil? avro-data#)
           (let [member-dispatch-name# (~class->name (class avro-data#))]
             {member-dispatch-name# (avro->clj member-dispatch-name#
                                               avro-data#)})))
      `(defmethod avro->clj ~dispatch-name
         [union-dispatch-name# avro-data#]
         (when-not (nil? avro-data#)
           (let [member-dispatch-name# (~class->name (class avro-data#))]
             (avro->clj member-dispatch-name# avro-data#)))))))

(defn fix-name [edn-schema]
  (-> edn-schema
      (update :namespace namespace-munge)
      (update :name #(csk/->PascalCase (name %)))))

(defmethod fix-default :fixed
  [field-schema default]
  (u/byte-array->byte-str default))

(defmethod fix-default :bytes
  [field-schema default]
  (u/byte-array->byte-str default))

(defmethod fix-default :enum
  [field-schema default]
  (-> (name default)
      (csk/->SCREAMING_SNAKE_CASE)))

(defmethod fix-default :record
  [default-schema default-record]
  (reduce (fn [acc field]
            (let [{field-name :name
                   field-type :type
                   field-default :default} field]
              (assoc acc field-name (fix-default field-type field-default))))
          {} (:fields default-schema)))

(defmethod fix-default :array
  [field-schema default]
  (let [child-schema (:items field-schema)]
    (mapv #(fix-default child-schema %) default)))

(defmethod fix-default :map
  [field-schema default]
  (let [child-schema (:values field-schema)]
    (reduce-kv (fn [acc k v]
                 (assoc k (fix-default child-schema v)))
               {} default)))

(defmethod fix-default :union
  [field-schema default]
  ;; Union default's type must be the first type in the union
  (fix-default (first field-schema) default))

(defmethod fix-default :default
  [field-schema default]
  default)

(defn fix-fields [edn-schema]
  (update edn-schema :fields
          (fn [fields]
            (mapv (fn [field]
                    (let [field-type (:type field)
                          avro-type (get-avro-type field-type)]
                      (cond-> field
                        true
                        (update :name #(csk/->camelCase (name %)))

                        (avro-complex-types avro-type)
                        (update :type edn-schema->avro-schema)

                        true
                        (update :default #(fix-default field-type %)))))
                  fields))))

(defn fix-symbols [edn-schema]
  (update edn-schema :symbols #(mapv csk/->SCREAMING_SNAKE_CASE %)))

(defmethod edn-schema->avro-schema :record
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-fields)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :enum
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-symbols)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :fixed
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :array
  [edn-schema]
  (update edn-schema :items edn-schema->avro-schema))

(defmethod edn-schema->avro-schema :map
  [edn-schema]
  (update edn-schema :values edn-schema->avro-schema))

(defmethod edn-schema->avro-schema :union
  [edn-schema]
  (mapv edn-schema->avro-schema edn-schema))

(defmethod edn-schema->avro-schema :string-reference
  [edn-schema]
  (let [name-parts (clojure.string/split edn-schema #"\.")
        schema-ns (clojure.string/join "." (butlast name-parts))
        schema-name (last name-parts)]
    (str (namespace-munge schema-ns) "." (csk/->PascalCase schema-name))))

(defmethod edn-schema->avro-schema :default
  [edn-schema]
  edn-schema)

(defn replace-nil-or-recur-schema [short-name args]
  (clojure.walk/postwalk
   (fn [form]
     (if (and (symbol? form)
              (= :__nil_or_recur_schema__ (eval form)))
       (let [edn-schema [:null short-name]
             record-dispatch-name (str *ns* "." short-name)
             union-dispatch-name (str "union-of-null," record-dispatch-name)
             constructor `(defmethod clj->avro ~union-dispatch-name
                            [union-dispatch-name# data#]
                            (when-not (nil? data#)
                              (clj->avro ~record-dispatch-name data#)))
             post-converter `(defmethod avro->clj ~union-dispatch-name
                               [union-dispatch-name# avro-data#]
                               (when-not (nil? avro-data#)
                                 (avro->clj ~record-dispatch-name avro-data#)))]
         `(do
            ~constructor
            ~post-converter
            ~edn-schema))
       form))
   args))

(defmacro schema-helper
  [schema-type clj-var-name args]
  (let [short-name (drop-schema-from-name clj-var-name)
        args (cond->> args
               (= :record schema-type) (replace-nil-or-recur-schema short-name)
               true (eval))
        schema-ns (str *ns*)
        java-ns (namespace-munge schema-ns)
        java-class-name (csk/->PascalCase short-name)
        class-expr (map symbol [java-ns java-class-name])
        full-java-name (str java-ns "." java-class-name)
        edn-schema (make-edn-schema schema-type schema-ns short-name args)
        dispatch-name (edn-schema->dispatch-name edn-schema)
        constructor (make-constructor edn-schema full-java-name dispatch-name)
        post-converter (make-post-converter edn-schema full-java-name
                                            dispatch-name)
        avro-schema (edn-schema->avro-schema edn-schema)
        json-schema (json/generate-string avro-schema {:pretty true})
        avro-type (get-avro-type edn-schema)]
    #?(:clj (when (avro-named-types avro-type)
              (gen/generate-classes json-schema)))
    `(let [avro-schema-obj# (.parse (Schema$Parser.) ^String ~json-schema)
           pf# (SchemaNormalization/toParsingForm avro-schema-obj#)
           fingerprint# (SchemaNormalization/parsingFingerprint
                         "MD5" avro-schema-obj#)
           writer# (SpecificDatumWriter. ^Schema avro-schema-obj#)
           reader# (SpecificDatumReader. ^Schema avro-schema-obj#)
           reader-cache# (sr/make-stockroom reader-cache-size)
           schema-obj# (->AvroSchema ~dispatch-name
                                     ~edn-schema ~json-schema avro-schema-obj#
                                     pf# writer# reader# fingerprint#
                                     reader-cache#)]
       (when (avro-named-types ~avro-type)
         (import ~class-expr))
       ~constructor
       ~post-converter
       (def ~clj-var-name schema-obj#))))
