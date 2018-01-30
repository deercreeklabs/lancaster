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

(declare get-field-default)


(def WrappedData [(s/one s/Keyword "schema-name")
                  (s/one s/Any "data")])

(def RecordFieldDef [(s/one s/Keyword "field-name")
                     (s/one (s/protocol u/IAvroSchema) "field-schema")
                     (s/optional s/Any "field-default")])

(defrecord AvroSchema [schema-name edn-schema json-schema parsing-canonical-form
                       fingerprint64 serializer deserializer
                       *pcf->resolving-deserializer]
  u/IAvroSchema
  (serialize [this os data]
    (serializer os data []))
  (deserialize [this writer-pcf is]
    (if (= writer-pcf parsing-canonical-form)
      (deserializer is)
      (if-let [rd (@*pcf->resolving-deserializer writer-pcf)]
        (rd is)
        (let [rd (resolution/make-resolving-deserializer writer-pcf this)]
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
    fingerprint64))

;;;;;;;;;;;;;;;;;;;; Multimethods ;;;;;;;;;;;;;;;;;;;;

(defmulti make-edn-schema u/first-arg-dispatch)
(defmulti edn-schema->avro-schema u/avro-type-dispatch)
(defmulti validate-schema-args u/first-arg-dispatch)

;;;;;;;;;;;;;;;;;;;; Utility Fns ;;;;;;;;;;;;;;;;;;;;

(defn make-schema
  ([schema-type ns-name schema-name args]
   (let [name-kw (keyword ns-name schema-name)]
     (make-schema schema-type name-kw args)))
  ([schema-type name-kw args]
   (when (and (u/avro-named-types schema-type)
              (not (keyword? name-kw)))
     (let [fn-name (str "make-" (name schema-type) "-schema")]
       (throw (ex-info (str "First arg to " fn-name " must be a name keyword."
                            "The keyword can be namespaced or not.")
                       {:given-name-kw name-kw}))))
   (when-not (u/avro-primitive-types schema-type)
     (validate-schema-args schema-type args))
   (let [edn-schema (if (u/avro-primitive-types schema-type)
                      schema-type
                      (make-edn-schema schema-type name-kw args))
         avro-schema (if (u/avro-primitive-types schema-type)
                       (name schema-type)
                       (edn-schema->avro-schema edn-schema))
         json-schema (u/edn->json-string avro-schema)
         parsing-canonical-form (pcf/avro-schema->pcf avro-schema)
         fingerprint64 (fingerprint/fingerprint64 parsing-canonical-form)
         serializer (u/make-serializer edn-schema)
         deserializer (u/make-deserializer edn-schema)
         *pcf->resolving-deserializer (atom {})
         schema-name (u/get-schema-name edn-schema)]
     (->AvroSchema schema-name edn-schema json-schema parsing-canonical-form
                   fingerprint64 serializer deserializer
                   *pcf->resolving-deserializer))))

(defn make-primitive-schema [schema-kw]
  (make-schema schema-kw nil nil))

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
      (when-not (satisfies? u/IAvroSchema field-schema)
        (throw (ex-info "Second arg in field definition must be schema object."
                        {:given-field-schema field-schema})))
      ;; TODO: Add validation for default
      )))

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
  (when-not (satisfies? u/IAvroSchema items-schema)
    (throw (ex-info "Second arg to make-array-schema must be schema object."
                    {:given-items-schema items-schema}))))

(defmethod validate-schema-args :map
  [schema-type values-schema]
  (when-not (satisfies? u/IAvroSchema values-schema)
    (throw (ex-info "Second arg to make-map-schema must be schema object."
                    {:given-values-schema values-schema}))))

(defmethod validate-schema-args :union
  [schema-type member-schemas]
  (when-not (sequential? member-schemas)
    (throw (ex-info (str "Second arg to make-union-schema must be a sequence "
                         "of member schema objects.")
                    {:given-member-schemas member-schemas})))
  (doseq [member-schema member-schemas]
    (when-not (satisfies? u/IAvroSchema member-schema)
      (throw (ex-info "All member schemas in a union must be schema objects."
                      {:bad-member-schema member-schema}))))
  (when (u/illegal-union? member-schemas)
    (throw (ex-info "Illegal union schema." {:schema member-schemas}))))

(defn ensure-edn-schema [schema]
  (cond
    (satisfies? u/IAvroSchema schema)
    (u/get-edn-schema schema)

    (string? schema)
    (if (clojure.string/includes? schema ".")
      schema
      (str *ns* "." schema))

    (sequential? schema)
    (mapv ensure-edn-schema schema) ;; union

    :else
    schema))

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

(defn make-default-fixed-or-bytes [num-bytes default]
  (u/byte-array->byte-str (or default
                              (ba/byte-array (take num-bytes (repeat 0))))))

(defn get-field-default [field-schema field-default]
  (let [avro-type (u/get-avro-type field-schema)]
    (case avro-type
      :record (make-default-record field-schema field-default)
      :union (get-field-default (first field-schema) field-default)
      :fixed (make-default-fixed-or-bytes (:size field-schema) field-default)
      :bytes (make-default-fixed-or-bytes 0 field-default)
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

(defn make-record-field [[field-name field-schema field-default]]
  (let [field-edn-schema (u/get-edn-schema field-schema)]
    {:name (keyword field-name)
     :type field-edn-schema
     :default (get-field-default field-edn-schema
                                 field-default)}))

(defn make-named-schema-base [name-kw]
  (cond-> {:name (keyword (name name-kw))}
    (qualified-keyword? name-kw) (assoc :namespace
                                        (keyword (namespace name-kw)))))

(defmethod make-edn-schema :record
  [schema-type name-kw fields]
  (assoc (make-named-schema-base name-kw)
         :type :record
         :fields (mapv make-record-field fields)))

(defmethod make-edn-schema :enum
  [schema-type name-kw symbols]
  (assoc (make-named-schema-base name-kw)
         :type :enum
         :symbols symbols))

(defmethod make-edn-schema :fixed
  [schema-type name-kw size]
  (assoc (make-named-schema-base name-kw)
         :type :fixed
         :size size))

(defmethod make-edn-schema :array
  [schema-type name-kw items]
  {:type :array
   :items (ensure-edn-schema items)})

(defmethod make-edn-schema :map
  [schema-type name-kw values]
  {:type :map
   :values (ensure-edn-schema values)})

(defmethod make-edn-schema :union
  [schema-type name-kw member-schemas]
  (mapv ensure-edn-schema member-schemas))

(defn fix-name [edn-schema]
  (cond-> edn-schema
    (:namespace edn-schema)
    (update :namespace #(u/clj-namespace->java-namespace (name %)))

    true
    (update :name #(csk/->PascalCase (name %)))))

(defn fix-alias [alias-kw]
  (-> alias-kw name u/edn-name->avro-name))

(defn fix-aliases [edn-schema]
  (if (contains? edn-schema :aliases)
    (update edn-schema :aliases #(map fix-alias %))
    edn-schema))

(defmulti fix-default u/avro-type-dispatch)

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
                 (assoc acc k (fix-default child-schema v)))
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
                        (update :type edn-schema->avro-schema)

                        true
                        (update :default #(fix-default field-type %)))))
                  fields))))

(defn fix-symbols [edn-schema]
  (update edn-schema :symbols #(mapv csk/->SCREAMING_SNAKE_CASE %)))

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

(defmethod edn-schema->avro-schema :record
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-fields)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :enum
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-symbols)))

(defmethod edn-schema->avro-schema :fixed
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)))

(defmethod edn-schema->avro-schema :array
  [edn-schema]
  (-> (update edn-schema :items edn-schema->avro-schema)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :map
  [edn-schema]
  (-> (update edn-schema :values edn-schema->avro-schema)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :union
  [edn-schema]
  (-> (mapv edn-schema->avro-schema edn-schema)
      (fix-repeated-schemas)))

(defmethod edn-schema->avro-schema :string-reference
  [edn-schema]
  (u/edn-name->avro-name edn-schema))

(defmethod edn-schema->avro-schema :default
  [edn-schema]
  edn-schema)
