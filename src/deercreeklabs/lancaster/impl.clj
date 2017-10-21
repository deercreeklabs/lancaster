(ns deercreeklabs.lancaster.impl
  (:require
   [camel-snake-kebab.core :as csk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.stockroom :as sr]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  (:import
   (java.io ByteArrayOutputStream)
   (java.nio ByteBuffer)
   (org.apache.avro Schema Schema$Parser SchemaNormalization)
   (org.apache.avro.io BinaryDecoder Decoder DecoderFactory
                       Encoder EncoderFactory ResolvingDecoder)
   (org.apache.avro.specific SpecificDatumReader SpecificDatumWriter)))

(declare get-field-default)

(def resolver-cache-size 50)

(defmulti fix-default u/avro-type-dispatch)

(defmethod u/clj->avro :null
  [dispatch-name x]
  nil)

(defmethod u/clj->avro :boolean
  [dispatch-name x]
  x)

(defmethod u/clj->avro :int
  [dispatch-name n]
  (int n))

(defmethod u/clj->avro :long
  [dispatch-name x]
  (long x))

(defmethod u/clj->avro :double
  [dispatch-name x]
  (double x))

(defmethod u/clj->avro :float
  [dispatch-name n]
  (float n))

(defmethod u/clj->avro :string
  [dispatch-name x]
  x)

(defmethod u/clj->avro :bytes
  [dispatch-name ba]
  (ByteBuffer/wrap ba))

(defmethod u/avro->clj :null
  [dispatch-name x]
  nil)

(defmethod u/avro->clj :boolean
  [dispatch-name x]
  x)

(defmethod u/avro->clj :int
  [dispatch-name x]
  x)

(defmethod u/avro->clj :long
  [dispatch-name x]
  x)

(defmethod u/avro->clj :float
  [dispatch-name x]
  x)

(defmethod u/avro->clj :double
  [dispatch-name x]
  x)

(defmethod u/avro->clj :bytes
  [dispatch-name ^ByteBuffer bb]
  (.array bb))

(defmethod u/avro->clj :string
  [dispatch-name ^org.apache.avro.util.Utf8 s]
  (.toString s))

(def ^EncoderFactory encoder-factory (EncoderFactory/get))
(def ^DecoderFactory decoder-factory (DecoderFactory/get))

(defn serialize* [^SpecificDatumWriter writer java-data-obj]
  (let [^ByteArrayOutputStream output-stream (ByteArrayOutputStream.)
        ^Encoder encoder (.binaryEncoder encoder-factory output-stream nil)]
    (.write writer java-data-obj encoder)
    (.flush encoder)
    (.toByteArray output-stream)))

(defn get-resolving-reader [reader-schema-obj writer-json-schema resolver-cache]
  (or (sr/get resolver-cache writer-json-schema)
      (let [writer-schema-obj (.parse ^Schema$Parser (Schema$Parser.)
                                      ^String writer-json-schema)
            resolving-reader (SpecificDatumReader. writer-schema-obj
                                                   reader-schema-obj)]
        (sr/put resolver-cache writer-json-schema resolving-reader)
        resolving-reader)))

(defn deserialize-resolving*
  [reader-schema-obj writer-json-schema resolver-cache ba]
  (let [^BinaryDecoder decoder (.binaryDecoder decoder-factory ^bytes ba nil)
        resolving-reader (get-resolving-reader
                          reader-schema-obj writer-json-schema resolver-cache)]
    (.read ^SpecificDatumReader resolving-reader nil decoder)))

(defn deserialize* [^SpecificDatumReader reader ba]
  (let [^BinaryDecoder decoder (.binaryDecoder decoder-factory ^bytes ba nil)]
    (.read reader nil decoder)))

(defrecord AvroSchema [dispatch-name edn-schema json-schema avro-schema-obj
                       parsing-canonical-form writer reader fingerprint128
                       resolver-cache]
  u/IAvroSchema
  (serialize [this data]
    (let [avro (u/clj->avro dispatch-name data)]
      (serialize* writer avro)))
  (deserialize [this writer-json-schema ba return-java?]
    (let [deser-fn (if (= writer-json-schema json-schema)
                     deserialize*
                     deserialize-resolving*)
          obj (if (= writer-json-schema json-schema)
                (deserialize* reader ba)
                (deserialize-resolving* avro-schema-obj writer-json-schema
                                        resolver-cache ba))]
      (if return-java?
        obj
        (u/avro->clj dispatch-name obj))))
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

(defn make-default-record [record-edn-schema default-record]
  (reduce (fn [acc field]
            (let [{field-name :name
                   field-type :type
                   field-default :default} field
                  field-schema (u/ensure-edn-schema field-type)
                  v (get-field-default field-schema
                                       (field-name default-record))]
              (assoc acc field-name v)))
          {} (:fields record-edn-schema)))

(defn make-default-fixed-or-bytes [default]
  (u/byte-array->byte-str (or default (ba/byte-array []))))

(defn get-field-default [field-schema field-default]
  (let [avro-type (u/get-avro-type field-schema)]
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

(defn get-name-or-schema [edn-schema *names]
  (let [schema-name (u/get-schema-name edn-schema)]
    (if (@*names schema-name)
      schema-name
      (do
        (swap! *names conj schema-name)
        edn-schema))))

(defn fix-repeated-schemas
  ([edn-schema]
   (fix-repeated-schemas edn-schema (atom #{})))
  ([edn-schema *names]
   (case (u/get-avro-type edn-schema)
     :enum (get-name-or-schema edn-schema *names)
     :fixed (get-name-or-schema edn-schema *names)
     :array (update edn-schema :items #(fix-repeated-schemas % *names))
     :map (update edn-schema :values #(fix-repeated-schemas % *names))
     :union (mapv #(fix-repeated-schemas % *names) edn-schema)
     :record (let [name-or-schema (get-name-or-schema edn-schema *names)
                   fix-field (fn [field]
                               (update field :type
                                       #(fix-repeated-schemas % *names)))]
               (if (map? name-or-schema)
                 (update edn-schema :fields #(mapv fix-field %))
                 name-or-schema))
     edn-schema)))

(defmethod u/make-edn-schema :primitive
  [schema-type schema-ns short-name primitive-type]
  primitive-type)

(defn make-field [[field-name field-type field-default]]
  (let [field-schema (u/ensure-edn-schema field-type)]
    {:name (keyword field-name)
     :type field-schema
     :default (get-field-default field-schema
                                 field-default)}))

(defmethod u/make-edn-schema :record
  [schema-type schema-ns short-name args]
  (let [base {:namespace schema-ns
              :name (keyword short-name)
              :type :record}]
    (if (map? (first args))
      (-> (merge base (first args))
          (update :fields #(mapv make-field %)))
      (assoc base :fields (mapv make-field args)))))

(defmethod u/make-edn-schema :enum
  [schema-type schema-ns short-name symbols]
  {:namespace schema-ns
   :name (keyword short-name)
   :type :enum
   :symbols symbols})

(defmethod u/make-edn-schema :fixed
  [schema-type schema-ns short-name size]
  {:namespace schema-ns
   :name (keyword short-name)
   :type :fixed
   :size size})

(defmethod u/make-edn-schema :array
  [schema-type schema-ns short-name items]
  {:type :array
   :items (u/ensure-edn-schema items)})

(defmethod u/make-edn-schema :map
  [schema-type schema-ns short-name values]
  {:type :map
   :values (u/ensure-edn-schema values)})

(defmethod u/make-edn-schema :union
  [schema-type _ _ member-schemas]
  (mapv u/ensure-edn-schema member-schemas))

(defn need-pre-conversion? [avro-type]
  ((conj u/avro-complex-types :bytes :int :float) avro-type))

(defmethod u/make-constructor :default
  [edn-schema full-java-name dispatch-name]
  ;; nil means don't generate a constructor
  nil)

(defmethod u/make-constructor :record
  [edn-schema full-java-name dispatch-name]
  (let [{:keys [fields]} edn-schema
        builder (symbol (str full-java-name "/newBuilder"))
        map-sym (gensym "map")
        mk-setter #(let [field-schema (u/ensure-edn-schema (:type %))
                         avro-type (u/get-avro-type field-schema)
                         name-kw (:name %)
                         field-dispatch-name (u/edn-schema->dispatch-name
                                              field-schema)
                         setter (symbol (str ".set"
                                             (csk/->PascalCase (name name-kw))))
                         getter (if (need-pre-conversion? avro-type)
                                  `(u/clj->avro ~field-dispatch-name
                                                (~name-kw ~map-sym))
                                  `(~name-kw ~map-sym))]
                     `((~name-kw ~map-sym) (~setter ~getter)))
        setters (-> (mapcat mk-setter fields)
                    (concat `(true (.build))))]
    `(defmethod u/clj->avro ~dispatch-name
       [_# ~map-sym]
       (cond-> (~builder)
         ~@setters))))

(defmethod u/make-constructor :enum
  [edn-schema full-java-name dispatch-name]
  (let [make-pair (fn [symbol-kw]
                    (let [symbol-str (csk/->SCREAMING_SNAKE_CASE
                                      (name symbol-kw))
                          java-enum (symbol (str full-java-name "/"
                                                 symbol-str))]
                      `(~symbol-kw ~java-enum)))
        pairs (mapcat make-pair (:symbols edn-schema))]
    `(defmethod u/clj->avro ~dispatch-name
       [_# enum-kw#]
       (case enum-kw#
         ~@pairs))))

(defmethod u/make-constructor :fixed
  [edn-schema full-java-name dispatch-name]
  (let [java-constructor (symbol (str full-java-name "."))]
    `(defmethod u/clj->avro ~dispatch-name
       [_# ba#]
       (~java-constructor ba#))))

(defmethod u/make-constructor :array
  [edn-schema _ dispatch-name]
  (let [{:keys [items]} edn-schema
        items-dispatch-name (u/edn-schema->dispatch-name items)
        items-avro-type (u/get-avro-type items)]
    (if (need-pre-conversion? items-avro-type)
      `(defmethod u/clj->avro ~dispatch-name
         [dispatch-name# arr#]
         (mapv #(u/clj->avro ~items-dispatch-name %) arr#))
      `(defmethod u/clj->avro ~dispatch-name
         [dispatch-name# arr#]
         arr#))))

(defmethod u/make-constructor :map
  [edn-schema _ dispatch-name]
  (let [{:keys [values]} edn-schema
        values-dispatch-name (u/edn-schema->dispatch-name values)
        values-avro-type (u/get-avro-type values)]
    (if (need-pre-conversion? values-avro-type)
      `(defmethod u/clj->avro ~dispatch-name
         [dispatch-name# m#]
         (reduce-kv (fn [acc# k# v#]
                      (assoc acc# k# (u/clj->avro ~values-dispatch-name v#)))
                    {} m#))
      `(defmethod u/clj->avro ~dispatch-name
         [dispatch-name# m#]
         m#))))

(defn make-data->dispatch-cond-line [data-sym member-schema]
  ;; Note that we only care about putting things in the right
  ;; category, as this is only used by unwrapped unions
  (let [dispatch-name (u/edn-schema->dispatch-name member-schema)]
    (case (u/get-avro-type member-schema)
      :null `((nil? ~data-sym) ~dispatch-name)
      :boolean `((boolean? ~data-sym) ~dispatch-name)
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

(defmethod u/make-constructor :union
  [edn-schema _ dispatch-name]
  (if (u/wrapping-required? edn-schema)
    `(defmethod u/clj->avro ~dispatch-name
       [dispatch-name# wrapped-data#]
       (when-not (nil? wrapped-data#)
         (try
           (apply u/clj->avro (first wrapped-data#))
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
      `(defmethod u/clj->avro ~dispatch-name
         [dispatch-name# ~data-sym]
         (when-not (nil? ~data-sym)
           (u/clj->avro (cond ~@cond-lines) ~data-sym))))))

(defn need-post-conversion? [avro-type]
  ((conj u/avro-complex-types :bytes :string) avro-type))

(defmethod u/make-post-converter :default
  [edn-schema full-java-name dispatch-name]
  ;; nil means don't generate a post-converter
  nil)

(defmethod u/make-post-converter :record
  [edn-schema full-java-name dispatch-name]
  (let [avro-obj-sym (symbol "avro-obj")
        make-kv (fn [field]
                  (let [field-schema (:type field)
                        avro-type (u/get-avro-type field-schema)
                        dispatch-name (u/edn-schema->dispatch-name field-schema)
                        name-kw (:name field)
                        getter (->> name-kw
                                    (name)
                                    (csk/->PascalCase)
                                    (str ".get")
                                    (symbol))
                        get-expr (if (need-post-conversion? avro-type)
                                   `(u/avro->clj ~dispatch-name
                                                 (~getter ~avro-obj-sym))
                                   `(~getter ~avro-obj-sym))
                        class-nk (class name-kw)
                        class-ge (class get-expr)]
                    `(~name-kw ~get-expr)))
        m (apply hash-map (mapcat make-kv (:fields edn-schema)))]
    `(defmethod u/avro->clj ~dispatch-name
       [dispatch-name# ~(with-meta avro-obj-sym {:tag (symbol full-java-name)})]
       ~m)))

(defmethod u/make-post-converter :enum
  [edn-schema full-java-name dispatch-name]
  (let [make-pair (fn [symbol-kw]
                    (let [symbol-str (csk/->SCREAMING_SNAKE_CASE
                                      (name symbol-kw))
                          java-enum (symbol (str full-java-name "/"
                                                 symbol-str))]
                      `(~java-enum ~symbol-kw)))
        pairs (mapcat make-pair (:symbols edn-schema))]
    `(defmethod u/avro->clj ~dispatch-name
       [dispatch-name# java-enum#]
       (condp = java-enum#
         ~@pairs))))

(defmethod u/make-post-converter :fixed
  [edn-schema full-java-name dispatch-name]
  (let [avro-obj-sym (symbol "avro-obj")]
    `(defmethod u/avro->clj ~dispatch-name
       [dispatch-name# ~(with-meta avro-obj-sym {:tag (symbol full-java-name)})]
       (.bytes ~avro-obj-sym))))

(defmethod u/make-post-converter :array
  [edn-schema _ dispatch-name]
  (let [{:keys [items]} edn-schema
        items-dispatch-name (u/edn-schema->dispatch-name items)
        items-avro-type (u/get-avro-type items)]
    (if (need-post-conversion? items-avro-type)
      `(defmethod u/avro->clj ~dispatch-name
         [dispatch-name# arr#]
         (mapv #(u/avro->clj ~items-dispatch-name %) arr#))
      `(defmethod u/avro->clj ~dispatch-name
         [dispatch-name# arr#]
         arr#))))

(defmethod u/make-post-converter :map
  [edn-schema _ dispatch-name]
  (let [{:keys [values]} edn-schema
        values-dispatch-name (u/edn-schema->dispatch-name values)
        values-avro-type (u/get-avro-type values)]
    (if (need-post-conversion? values-avro-type)
      `(defmethod u/avro->clj ~dispatch-name
         [dispatch-name# m#]
         (reduce (fn [acc# [^org.apache.avro.util.Utf8 k# v#]]
                   (assoc acc# (.toString k#)
                          (u/avro->clj ~values-dispatch-name v#)))
                 {} m#))
      `(defmethod u/avro->clj ~dispatch-name
         [dispatch-name# m#]
         (reduce (fn [acc# [^org.apache.avro.util.Utf8 k# v#]]
                   (assoc acc# (.toString k#) v#))
                 {} m#)))))

(defn named-schema->java-class [edn-schema]
  (let [java-ns (namespace-munge (:namespace edn-schema))
        java-class-name (csk/->PascalCase (name (:name edn-schema)))]
    (Class/forName (str java-ns "." java-class-name))))

(defn edn-schema->java-class [edn-schema]
  (case (u/get-avro-type edn-schema)
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
    (named-schema->java-class edn-schema)))

(defn make-class->name [union-schema]
  (let [java-classes (map edn-schema->java-class union-schema)
        dispatch-names (map u/edn-schema->dispatch-name union-schema)]
    (zipmap java-classes dispatch-names)))

(defmethod u/make-post-converter :union
  [edn-schema _ dispatch-name]
  (let [class->name (make-class->name edn-schema)]
    (if (u/wrapping-required? edn-schema)
      `(defmethod u/avro->clj ~dispatch-name
         [union-dispatch-name# avro-data#]
         (when-not (nil? avro-data#)
           (let [member-dispatch-name# (~class->name (class avro-data#))]
             {member-dispatch-name# (u/avro->clj member-dispatch-name#
                                                 avro-data#)})))
      `(defmethod u/avro->clj ~dispatch-name
         [union-dispatch-name# avro-data#]
         (when-not (nil? avro-data#)
           (let [member-dispatch-name# (~class->name (class avro-data#))]
             (u/avro->clj member-dispatch-name# avro-data#)))))))

(defn fix-name [edn-schema]
  (-> edn-schema
      (update :namespace namespace-munge)
      (update :name #(csk/->PascalCase (name %)))))

(defn fix-alias [alias-kw]
  (-> alias-kw name u/edn-name->avro-name))

(defn fix-aliases [edn-schema]
  (if (contains? edn-schema :aliases)
    (update edn-schema :aliases #(map fix-alias %))
    edn-schema))

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
                          avro-type (u/get-avro-type field-type)]
                      (cond-> field
                        true
                        (update :name #(csk/->camelCase (name %)))

                        (u/avro-complex-types avro-type)
                        (update :type u/edn-schema->avro-schema)

                        true
                        (update :default #(fix-default field-type %)))))
                  fields))))

(defn fix-symbols [edn-schema]
  (update edn-schema :symbols #(mapv csk/->SCREAMING_SNAKE_CASE %)))

(defmethod u/edn-schema->avro-schema :record
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-fields)
      (fix-repeated-schemas)))

(defmethod u/edn-schema->avro-schema :enum
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-symbols)
      (fix-repeated-schemas)))

(defmethod u/edn-schema->avro-schema :fixed
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-repeated-schemas)))

(defmethod u/edn-schema->avro-schema :array
  [edn-schema]
  (update edn-schema :items u/edn-schema->avro-schema))

(defmethod u/edn-schema->avro-schema :map
  [edn-schema]
  (update edn-schema :values u/edn-schema->avro-schema))

(defmethod u/edn-schema->avro-schema :union
  [edn-schema]
  (mapv u/edn-schema->avro-schema edn-schema))

(defmethod u/edn-schema->avro-schema :string-reference
  [edn-schema]
  (u/edn-name->avro-name edn-schema))

(defmethod u/edn-schema->avro-schema :default
  [edn-schema]
  edn-schema)

(defn make-schema-obj
  [dispatch-name edn-schema json-schema]
  (let [avro-schema-obj (.parse (Schema$Parser.) ^String json-schema)
        pf (SchemaNormalization/toParsingForm avro-schema-obj)
        fingerprint (SchemaNormalization/parsingFingerprint
                     "MD5" avro-schema-obj)
        writer (SpecificDatumWriter. ^Schema avro-schema-obj)
        reader (SpecificDatumReader. ^Schema avro-schema-obj)
        resolver-cache (sr/make-stockroom resolver-cache-size)]
    (->AvroSchema dispatch-name edn-schema json-schema avro-schema-obj
                  pf writer reader fingerprint resolver-cache)))
