(ns deercreeklabs.lancaster.schemas
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.fingerprint :as fingerprint]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.resolution :as resolution]
   [deercreeklabs.lancaster.pcf :as pcf]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:clj (pm/use-primitive-operators))

(def WrappedData [(s/one s/Keyword "schema-name")
                  (s/one s/Any "data")])

(def RecordFieldDef [(s/one s/Keyword "field-name")
                     (s/one (s/protocol u/ILancasterSchema) "field-schema")
                     (s/optional s/Any "field-default")])

(defrecord LancasterSchema
    [schema-name edn-schema json-schema parsing-canonical-form
     fingerprint64 plumatic-schema serializer deserializer default-data-size
     *name->serializer *name->deserializer *pcf->resolving-deserializer]
  u/ILancasterSchema
  (serialize [this data]
    (let [os (impl/make-output-stream default-data-size)]
      (u/serialize this os data)
      (u/to-byte-array os)))
  (serialize [this os data]
    (serializer os data []))
  (deserialize [this writer-pcf is]
    (if (= writer-pcf parsing-canonical-form)
      (deserializer is)
      (if-let [rd (@*pcf->resolving-deserializer writer-pcf)]
        (rd is)
        (let [rd (resolution/make-resolving-deserializer writer-pcf this
                                                         *name->deserializer)]
          (swap! *pcf->resolving-deserializer assoc writer-pcf rd)
          (rd is)))))
  (wrap [this data]
    [schema-name data])
  (get-edn-schema [this]
    edn-schema)
  (get-json-schema [this]
    json-schema)
  (get-parsing-canonical-form [this]
    parsing-canonical-form)
  (get-fingerprint64 [this]
    fingerprint64)
  (get-plumatic-schema [this]
    plumatic-schema))

(defmulti validate-schema-args u/first-arg-dispatch)

(defn edn-schema->lancaster-schema [schema-type edn-schema]
  (let [name->edn-schema (u/make-name->edn-schema edn-schema)
        avro-schema (if (u/avro-primitive-types schema-type)
                      (name schema-type)
                      (u/edn-schema->avro-schema edn-schema))
        json-schema (u/edn->json-string avro-schema)
        parsing-canonical-form (pcf/avro-schema->pcf avro-schema)
        fingerprint64 (fingerprint/fingerprint64 parsing-canonical-form)
        plumatic-schema (u/edn-schema->plumatic-schema edn-schema
                                                       name->edn-schema)
        *name->serializer (atom {})
        *name->deserializer (atom {})
        serializer (u/make-serializer edn-schema name->edn-schema
                                      *name->serializer)
        deserializer (u/make-deserializer edn-schema *name->deserializer)
        default-data-size (u/make-default-data-size edn-schema
                                                    name->edn-schema)
        *pcf->resolving-deserializer (atom {})
        schema-name (u/get-schema-name edn-schema)]
    (->LancasterSchema
     schema-name edn-schema json-schema parsing-canonical-form
     fingerprint64 plumatic-schema serializer deserializer default-data-size
     *name->serializer *name->deserializer *pcf->resolving-deserializer)))

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

(defn validate-name-kw [name-kw]
  (when-not (re-matches #"[A-Za-z][A-Za-z0-9\-]*" (name name-kw))
    (throw (ex-info
            (str "Name keywords must start with a letter and "
                 "subsequently may only contain letters, numbers, "
                 "or hyphens")
            {:given-name-kw name-kw}))))

(defn make-schema
  ([schema-type ns-name schema-name args]
   (let [name-kw (keyword ns-name schema-name)]
     (make-schema schema-type name-kw args)))
  ([schema-type name-kw args]
   (when (u/avro-named-types schema-type)
     (when (not (keyword? name-kw))
       (let [fn-name (str "make-" (name schema-type) "-schema")]
         (throw (ex-info (str "First arg to " fn-name " must be a name keyword."
                              "The keyword can be namespaced or not.")
                         {:given-name-kw name-kw}))))
     (validate-name-kw name-kw))
   (when-not (u/avro-primitive-types schema-type)
     (validate-schema-args schema-type args))
   (let [edn-schema (if (u/avro-primitive-types schema-type)
                      schema-type
                      (-> (u/make-edn-schema schema-type name-kw args)
                          (fix-repeated-schemas)))]
     (edn-schema->lancaster-schema schema-type edn-schema ))))

(defn merge-record-schemas [name-kw schemas]
  (when-not (keyword? name-kw)
    (throw (ex-info (str "First arg to merge-record-schemas must be a name "
                         "keyword. The keyword can be namespaced or not.")
                    {:given-name-kw name-kw})))
  (when-not (sequential? schemas)
    (throw (ex-info (str "Second arg to merge-record-schemas must be a "
                         "sequence of record schema objects.")
                    {:given-schemas schemas})))
  (doseq [schema schemas]
    (when (or (not (instance? LancasterSchema schema))
              (not (= :record (:type (u/get-edn-schema schema)))))
      (throw (ex-info (str "Second arg to merge-record-schemas must be a "
                           "sequence of record schema objects.")
                      {:bad-schema schema}))))
  (let [fields (mapcat #(:fields (u/get-edn-schema %)) schemas)
        edn-schema {:name name-kw
                    :type :record
                    :fields fields}]
    (edn-schema->lancaster-schema :record edn-schema)))

(defn make-primitive-schema [schema-kw]
  (make-schema schema-kw nil nil))

(defn schema-or-kw? [x]
  (or (instance? LancasterSchema x)
      (keyword? x)))

(defmethod validate-schema-args :record
  [schema-type fields]
  (when-not (sequential? fields)
    (throw (ex-info (str "Second arg to make-record-schema must be a sequence "
                         "of field definitions.")
                    {:given-fields fields})))
  (doseq [field fields]
    (let [[name-kw field-schema default] field]
      (when-not (keyword? name-kw)
        (throw (ex-info "First arg in field definition must be a name keyword."
                        {:given-name-kw name-kw})))
      (validate-name-kw name-kw)
      (when-not (schema-or-kw? field-schema)
        (throw
         (ex-info (str "Second arg in field definition must be a schema object "
                       "or a name keyword.")
                  {:given-field-schema field-schema})))
      (when default
        (try
          (u/serialize field-schema (impl/make-output-stream 100) default)
          (catch #?(:clj Exception :cljs js/Error) e
            (let [ex-msg (lu/get-exception-msg e)]
              (if (str/includes? ex-msg "not a valid")
                (throw
                 (ex-info
                  (str "Default value for field `" name-kw "` is invalid. "
                       ex-msg)
                  (u/sym-map name-kw default ex-msg)))))))))))

(defmethod validate-schema-args :enum
  [schema-type symbols]
  (when-not (sequential? symbols)
    (throw (ex-info (str "Second arg to make-enum-schema must be a sequence "
                         "of keywords.")
                    {:given-symbols symbols})))
  (doseq [symbol symbols]
    (when-not (keyword? symbol)
      (throw (ex-info "All symbols in an enum must be keywords."
                      {:given-symbol symbol})))))

(defmethod validate-schema-args :fixed
  [schema-type size]
  (when-not (integer? size)
    (throw (ex-info (str "Second arg to make-fixed-schema (size) must be an "
                         "integer.")
                    {:given-size size}))))

(defmethod validate-schema-args :array
  [schema-type items-schema]
  (when-not (schema-or-kw? items-schema)
    (throw
     (ex-info (str "Arg to make-array-schema must be a schema object "
                   "or a name keyword.")
              {:given-items-schema items-schema}))))

(defmethod validate-schema-args :map
  [schema-type values-schema]
  (when-not (schema-or-kw? values-schema)
    (throw
     (ex-info (str "Arg to make-map-schema must be a schema object "
                   "or a name keyword.")
              {:given-values-schema values-schema}))))

(defmethod validate-schema-args :flex-map
  [schema-type [keys-schema values-schema]]
  (when-not (schema-or-kw? keys-schema)
    (throw
     (ex-info (str "Second arg to make-flex-map-schema must be a schema object "
                   "or a name keyword indicating the key schema of the "
                   "flex-map.")
              {:given-values-schema values-schema})))
  (when-not (schema-or-kw? values-schema)
    (throw
     (ex-info (str "Third arg to make-flex-map-schema must be a schema object "
                   "or a name keyword indicating the value schema of the "
                   "flex-map.")
              {:given-values-schema values-schema}))))

(defmethod validate-schema-args :union
  [schema-type member-schemas]
  (when-not (sequential? member-schemas)
    (throw (ex-info (str "Arg to make-union-schema must be a sequence "
                         "of member schema objects or name keywords.")
                    {:given-member-schemas member-schemas})))
  (doseq [member-schema member-schemas]
    (when-not (schema-or-kw? member-schema)
      (throw
       (ex-info (str "All member schemas in a union must be schema objects "
                     "or name keywords.")
                {:bad-member-schema member-schema}))))
  (when (u/illegal-union? (map u/get-edn-schema
                               ;; Name keywords & named schemas are always okay
                               (remove keyword? member-schemas)))
    (throw (ex-info "Illegal union schema." {:schema member-schemas}))))
