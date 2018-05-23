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
                     (s/one (s/protocol u/ILancasterSchema) "field-schema")
                     (s/optional s/Any "field-default")])

(defrecord LancasterSchema
    [schema-name edn-schema json-schema parsing-canonical-form
     fingerprint64 plumatic-schema serializer deserializer *name->serializer
     *name->deserializer *pcf->resolving-deserializer]
  u/ILancasterSchema
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

(defmulti make-edn-schema u/first-arg-dispatch)
(defmulti validate-schema-args u/first-arg-dispatch)

(defn edn-schema->lancaster-schema [schema-type edn-schema]
  (let [name->avro-type (u/make-name->avro-type edn-schema)
        avro-schema (if (u/avro-primitive-types schema-type)
                      (name schema-type)
                      (u/edn-schema->avro-schema edn-schema))
        json-schema (u/edn->json-string avro-schema)
        parsing-canonical-form (pcf/avro-schema->pcf avro-schema)
        fingerprint64 (fingerprint/fingerprint64 parsing-canonical-form)
        plumatic-schema (u/edn-schema->plumatic-schema edn-schema
                                                       name->avro-type)
        *name->serializer (atom {})
        *name->deserializer (atom {})
        serializer (u/make-serializer edn-schema name->avro-type
                                      *name->serializer)
        deserializer (u/make-deserializer edn-schema *name->deserializer)
        *pcf->resolving-deserializer (atom {})
        schema-name (u/get-schema-name edn-schema)]
    (->LancasterSchema
     schema-name edn-schema json-schema parsing-canonical-form
     fingerprint64 plumatic-schema serializer deserializer *name->serializer
     *name->deserializer *pcf->resolving-deserializer)))

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
                      (-> (make-edn-schema schema-type name-kw args)
                          (fix-repeated-schemas)))]
     (edn-schema->lancaster-schema schema-type edn-schema ))))

(defn merge-record-schemas [name-kw schemas]
  (when-not (keyword? name-kw)
    (throw (ex-info (str "First arg to merge-record-schemas must be a name "
                         "keyword. The keyword can be namespaced or not.")
                    {:given-name-kw name-kw})))

  (let [fields (->> (mapcat #(:fields (u/get-edn-schema %))
                            schemas)
                    (mapv (fn [{:keys [name type default]}]
                            [name type default])))]
    (make-schema :record name-kw fields)))

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
      (when-not (schema-or-kw? field-schema)
        (throw
         (ex-info (str "Second arg in field definition must be a schema object "
                       "or a name keyword.")
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
  (when-not (schema-or-kw? items-schema)
    (throw
     (ex-info (str "Second arg to make-array-schema must be a schema object "
                   "or a name keyword.")
              {:given-items-schema items-schema}))))

(defmethod validate-schema-args :map
  [schema-type values-schema]
  (when-not (schema-or-kw? values-schema)
    (throw
     (ex-info (str "Second arg to make-map-schema must be a schema object "
                   "or a name keyword.")
              {:given-values-schema values-schema}))))

(defmethod validate-schema-args :union
  [schema-type member-schemas]
  (when-not (sequential? member-schemas)
    (throw (ex-info (str "Second arg to make-union-schema must be a sequence "
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

(defn ensure-edn-schema [schema]
  (cond
    (instance? LancasterSchema schema)
    (u/get-edn-schema schema)

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

(defn make-record-field [field]
  (when-not (#{2 3} (count field))
    (throw
     (ex-info (str "Record field definition must have 2 or 3 parameters. ("
                   "[field-name field-schema] or "
                   "[field-name field-schema field-default]).\n"
                   "  Got " (count field) " parameters.\n"
                   "  Bad field definition: " field)
              {:bad-field-def field})))
  (let [[field-name field-schema field-default] field
        field-edn-schema (if (keyword? field-schema)
                           field-schema
                           (u/get-edn-schema field-schema))]
    (when-not (keyword? field-name)
      (throw
       (ex-info (str "Field names must be keywords. Bad field name: "
                     field-name)
                (u/sym-map field-name field-schema field-default field))))
    {:name field-name
     :type field-edn-schema
     :default (get-field-default field-edn-schema field-default)}))

(defmethod make-edn-schema :record
  [schema-type name-kw fields]
  (let [name-kw (u/qualify-name-kw name-kw)
        fields (binding [u/**enclosing-namespace** (namespace name-kw)]
                 (mapv make-record-field fields))
        edn-schema {:name name-kw
                    :type :record
                    :fields fields}]
    edn-schema))

(defmethod make-edn-schema :enum
  [schema-type name-kw symbols]
  (let [name-kw (u/qualify-name-kw name-kw)
        edn-schema {:name name-kw
                    :type :enum
                    :symbols symbols}]
    edn-schema))

(defmethod make-edn-schema :fixed
  [schema-type name-kw size]
  (let [name-kw (u/qualify-name-kw name-kw)
        edn-schema {:name name-kw
                    :type :fixed
                    :size size}]
    edn-schema))

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
