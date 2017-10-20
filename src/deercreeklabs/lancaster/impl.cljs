(ns deercreeklabs.lancaster.impl
  (:require
   [camel-snake-kebab.core :as csk]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.stockroom :as sr]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def avsc (js/require "avsc"))
(def Type (goog.object.get avsc "Type"))
(def resolver-cache-size 50)

(defn json-schema->avro-schema-obj [json-schema]
  (.forSchema Type (js/JSON.parse json-schema)))

(defn get-resolver [reader-schema-obj writer-json-schema resolver-cache]
  (or (sr/get resolver-cache writer-json-schema)
      (let [writer-schema-obj (json-schema->avro-schema-obj writer-json-schema)
            resolver (.createResolver reader-schema-obj writer-schema-obj)]
        (sr/put resolver-cache writer-json-schema resolver)
        resolver)))

(defrecord AvroSchema [edn-schema edn-schema-name json-schema avro-schema-obj
                       parsing-canonical-form fingerprint128 resolver-cache
                       pre-converter post-converter]
  u/IAvroSchema
  (serialize [this data]
    (->> (pre-converter data)
         (.toBuffer avro-schema-obj)
         (js/Int8Array.)))
  (deserialize [this writer-json-schema ba return-native?]
    (let [obj (if (= writer-json-schema json-schema)
                (.fromBuffer avro-schema-obj (js/Buffer. ba))
                (let [resolver (get-resolver avro-schema-obj writer-json-schema
                                             resolver-cache)]
                  (.fromBuffer avro-schema-obj (js/Buffer. ba) resolver)))]
      (if return-native?
        obj
        (post-converter obj))))
  (wrap [this data]
    {edn-schema-name data})
  (get-edn-schema [this]
    edn-schema)
  (get-json-schema [this]
    json-schema)
  (get-parsing-canonical-form [this]
    parsing-canonical-form)
  (get-fingerprint128 [this]
    fingerprint128))

(defmulti make-pre-converter u/avro-type-dispatch)

;; For use in recursive record schemas
(def ^:dynamic **parent-pre-converter** identity)

(defmethod make-pre-converter :default
  [edn-schema]
  identity)

(defmethod make-pre-converter :record
  [edn-schema]
  (let [{:keys [fields]} edn-schema
        clj-ks (map :name fields)
        js-ks (map #(csk/->camelCase (name %)) clj-ks)
        field-converters (mapv #(make-pre-converter (:type %)) fields)
        infos (map vector js-ks field-converters)
        clj-k->info (zipmap clj-ks infos)
        get-js-kvp (fn [[clj-k clj-v]]
                     (let [[js-k converter] (clj-k->info clj-k)]
                       [js-k (converter clj-v)]))]
    (fn pre-convert [m]
      (binding [**parent-pre-converter** pre-convert]
        (let [kvps (mapcat get-js-kvp m)]
          (apply js-obj kvps))))))

(defmethod make-pre-converter :enum
  [edn-schema]
  (let [kws (:symbols edn-schema)
        strs (map #(csk/->SCREAMING_SNAKE_CASE (name %)) kws)
        kw->str (zipmap kws strs)]
    (fn [enum-kw]
      (kw->str enum-kw))))

(defmethod make-pre-converter :fixed
  [edn-schema]
  (fn [ba]
    (js/Buffer. ba)))

(defmethod make-pre-converter :bytes
  [edn-schema]
  (fn [ba]
    (js/Buffer. ba)))

(defmethod make-pre-converter :map
  [edn-schema]
  (let [{:keys [values]} edn-schema
        converter (make-pre-converter values)
        xf (fn [[k v]]
                [k (converter v)])]
    (fn [m]
      (apply js-obj (mapcat xf m)))))

(defmethod make-pre-converter :array
  [edn-schema]
  (let [{:keys [items]} edn-schema
        converter (make-pre-converter items)]
    (fn [a]
      (apply array (map converter a)))))

(defmethod make-pre-converter :string-reference
  [edn-schema]
  (fn [nil-or-record]
    (when-not (nil? nil-or-record)
      (**parent-pre-converter** nil-or-record))))

(defn make-clj-test-name-pair [member-schema]
  (let [schema-name (u/get-schema-name member-schema)
        test (case (u/get-avro-type member-schema)
               :null nil?
               :boolean boolean?
               :int number?
               :long number?
               :float number?
               :double number?
               :bytes ba/byte-array?
               :string string?
               :record map?
               :fixed ba/byte-array?
               :enum string?
               :array sequential?
               :map map?
               ;; A string reference must refer to a record, which is
               ;; a map
               :string-reference map?
               ;; Since unions can't contain other unions, we don't
               ;; have a case for :union
               )]
    [test schema-name]))

(defn find-schema-name [v [test schema-name]]
  (when (test v)
    schema-name))

(defn make-avro-name [edn-schema]
  (cond
    (u/avro-named-types (:type edn-schema))
    (let [schema-ns (u/namespace-munge (:namespace edn-schema))
          schema-name (csk/->PascalCase (name (:name edn-schema)))]
      (if (count schema-ns)
        (str schema-ns "." schema-name)
        schema-name))

    (map? edn-schema)
    (name (:type edn-schema))

    (keyword? edn-schema)
    (name edn-schema)

    (string? edn-schema)
    edn-schema

    (nil? edn-schema)
    (throw (ex-info "Schema is nil."
                    {:type :illegal-argument
                     :subtype :schema-is-nil
                     :schema edn-schema}))

    :else
    (throw (ex-info (str "Unknown schema type: " edn-schema)
                    {:type :illegal-argument
                     :subtype :unknown-schema-type
                     :schema edn-schema}))))

(defn make-clj-schema-name->avro-name [union-schema]
  (let [schema-names (map u/get-schema-name union-schema)
        avro-names (map make-avro-name union-schema)]
    (zipmap schema-names avro-names)))

(defmethod make-pre-converter :union
  [edn-schema]
  (let [schema-names (map u/get-schema-name edn-schema)
        converters (map make-pre-converter edn-schema)
        clj-schema-name->converter (zipmap schema-names converters)
        clj-schema-name->avro-name (make-clj-schema-name->avro-name
                                      edn-schema)]
    (if (u/wrapping-required? edn-schema)
      (fn [wrapped-clj-v]
        (let [[clj-schema-name clj-v] (first wrapped-clj-v)
              converter (clj-schema-name->converter clj-schema-name)
              avro-name (clj-schema-name->avro-name clj-schema-name)]
          (js-obj avro-name (converter clj-v))))
      (let [test-name-pairs (map make-clj-test-name-pair edn-schema)]
        (fn [clj-v]
          (let [schema-name (some (partial find-schema-name clj-v)
                                    test-name-pairs)
                converter (clj-schema-name->converter schema-name)]
            (converter clj-v)))))))

(defmulti make-post-converter u/avro-type-dispatch)

;; For use in recursive record schemas
(def ^:dynamic **parent-post-converter** identity)

(defmethod make-post-converter :default
  [edn-schema]
  identity)

(defmethod make-post-converter :record
  [edn-schema]
  (let [{:keys [fields]} edn-schema
        clj-ks (map :name fields)
        js-ks (map #(csk/->camelCase (name %)) clj-ks)
        field-converters (mapv #(make-post-converter (:type %)) fields)
        get-clj-val (fn [i v]
                      ((field-converters i) v))]
    (fn post-convert [js-m]
      (binding [**parent-post-converter** post-convert]
        (let [js-vals (map #(goog.object.get js-m %) js-ks)
              clj-vals (map-indexed get-clj-val js-vals)]
          (zipmap clj-ks clj-vals))))))

(defmethod make-post-converter :enum
  [edn-schema]
  (let [kws (:symbols edn-schema)
        strs (map #(csk/->SCREAMING_SNAKE_CASE (name %)) kws)
        str->kw (zipmap strs kws)]
    (fn [enum-str]
      (str->kw enum-str))))

(defmethod make-post-converter :fixed
  [edn-schema]
  (fn [buffer]
    (js/Int8Array. buffer)))

(defmethod make-post-converter :bytes
  [edn-schema]
  (fn [buffer]
    (js/Int8Array. buffer)))

(defmethod make-post-converter :map
  [edn-schema]
  (let [{:keys [values]} edn-schema
        converter (make-post-converter values)
        xf (fn [[k v]]
                [k (converter v)])]
    (fn [js-obj]
      (let [entries (.entries js/Object js-obj)
            kvps (mapcat xf entries)]
        (apply hash-map kvps)))))

(defmethod make-post-converter :array
  [edn-schema]
  (let [{:keys [items]} edn-schema
        converter (make-post-converter items)]
    (fn [js-arr]
      (mapv converter js-arr))))

(defmethod make-post-converter :string-reference
  [edn-schema]
  (fn [nil-or-record]
    (when-not (nil? nil-or-record)
      (**parent-post-converter** nil-or-record))))

(defn js-map? [x]
  (and (instance? js/Object x)
       (not (instance? js/Array x))))

(defn make-js-test-name-pair [member-schema]
  (let [schema-name (u/get-schema-name member-schema)
        test (case (u/get-avro-type member-schema)
               :null nil?
               :boolean boolean?
               :int number?
               :long number?
               :float number?
               :double number?
               :bytes #(instance? js/Buffer %)
               :string string?
               :record js-map?
               :fixed  #(instance? js/Buffer %)
               :enum string?
               :array #(instance? js/Array %)
               :map js-map?
               :string-reference js-map?
               ;; Since unions can't contain other unions, we don't
               ;; have a case for :union
               )]
    [test schema-name]))

(defn make-avro-name->clj-schema-name [union-schema]
  (let [avro-names (map make-avro-name union-schema)
        schema-names (map u/get-schema-name union-schema)]
    (zipmap avro-names schema-names)))

(defmethod make-post-converter :union
  [edn-schema]
  (let [schema-names (mapv u/get-schema-name edn-schema)
        converters (mapv make-post-converter edn-schema)
        schema-name->converter (zipmap schema-names converters)
        avro-name->clj-schema-name (make-avro-name->clj-schema-name
                                      edn-schema)]
    (if (u/wrapping-required? edn-schema)
      (fn [wrapped-js-v]
        (let [[avro-name js-v] (first (.entries js/Object wrapped-js-v))
              schema-name (avro-name->clj-schema-name avro-name)
              converter (schema-name->converter schema-name)]
          {schema-name (converter js-v)}))
      (let [test-name-pairs (mapv make-js-test-name-pair edn-schema)]
        (fn [js-v]
          (let [schema-name (some (partial find-schema-name js-v)
                                    test-name-pairs)
                converter (schema-name->converter schema-name)]
            (converter js-v)))))))

(defn make-schema-obj [edn-schema json-schema]
  (let [edn-schema-name (u/get-schema-name edn-schema)
        avro-schema-obj (json-schema->avro-schema-obj json-schema)
        parsing-canonical-form (js/JSON.stringify (.schema avro-schema-obj))
        fp (js/Int8Array. (.fingerprint avro-schema-obj "md5"))
        resolver-cache (sr/make-stockroom resolver-cache-size)
        pre-converter (make-pre-converter edn-schema)
        post-converter (make-post-converter edn-schema)]
    (->AvroSchema edn-schema edn-schema-name json-schema avro-schema-obj
                  parsing-canonical-form fp resolver-cache pre-converter
                  post-converter)))
