(ns deercreeklabs.lancaster.schemas
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.deser :as deser]
   [deercreeklabs.lancaster.fingerprint :as fingerprint]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.pcf-utils :as pcf-utils]
   [deercreeklabs.lancaster.utils :as u]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]))

#?(:clj (pm/use-primitive-operators))

(def LancasterSchemaOrNameKW (s/if keyword?
                               s/Keyword
                               (s/protocol u/ILancasterSchema)))
(def RecordFieldDef [(s/one s/Keyword "field-name")
                     (s/one LancasterSchemaOrNameKW "field-schema")
                     (s/optional s/Any "field-default")])

(defrecord LancasterSchema
    [edn-schema name->edn-schema json-schema parsing-canonical-form
     fingerprint64 plumatic-schema serializer default-data-size
     *name->serializer *writer-fp->deserializer]
  u/ILancasterSchema
  (serialize [this data]
    (let [os (impl/output-stream default-data-size)]
      (u/serialize this os data)
      (u/to-byte-array os)))
  (serialize [this os data]
    (serializer os data []))
  (deserialize [this writer-schema is]
    (try
      (let [writer-fp (u/fingerprint64 writer-schema)
            deser (or (@*writer-fp->deserializer writer-fp)
                      (let [deser* (deser/make-deserializer
                                    (u/edn-schema writer-schema)
                                    edn-schema name->edn-schema (atom {}))]
                        (swap! *writer-fp->deserializer assoc writer-fp deser*)
                        deser*))]
        (deser is))
      (catch #?(:clj Exception :cljs js/Error) e
        (if-not (u/match-exception? e)
          (throw e)
          (let [msg (u/ex-msg e)]
            (throw (ex-info (str "Reader and writer schemas do not match. "
                                 msg)
                            {:writer-edn-schema (u/edn-schema writer-schema)
                             :reader-edn-schema edn-schema
                             :orig-e e
                             :orig-msg msg})))))))
  (edn-schema [this]
    edn-schema)
  (json-schema [this]
    json-schema)
  (parsing-canonical-form [this]
    parsing-canonical-form)
  (fingerprint64 [this]
    fingerprint64)
  (plumatic-schema [this]
    plumatic-schema))

(defmulti validate-schema-args u/first-arg-dispatch)
(defmulti make-edn-schema u/first-arg-dispatch)

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
        field-edn-schema (if (satisfies? u/ILancasterSchema field-schema)
                           (u/edn-schema field-schema)
                           field-schema)]
    (when-not (keyword? field-name)
      (throw
       (ex-info (str "Field names must be keywords. Bad field name: "
                     field-name)
                (u/sym-map field-name field-schema field-default field))))
    {:name field-name
     :type field-edn-schema
     :default (u/default-data field-edn-schema field-default)}))

(defmethod make-edn-schema :record
  [schema-type name-kw fields]
  (let [name-kw (u/qualify-name-kw name-kw)
        fields (binding [u/**enclosing-namespace** (namespace name-kw)]
                 (mapv make-record-field fields))]
    {:name name-kw
     :type :record
     :fields fields}))

(defmethod make-edn-schema :enum
  [schema-type name-kw symbols]
  (let [name-kw (u/qualify-name-kw name-kw)]
    {:name name-kw
     :type :enum
     :symbols symbols
     :default (first symbols)}))

(defmethod make-edn-schema :fixed
  [schema-type name-kw size]
  (let [name-kw (u/qualify-name-kw name-kw)]
    {:name name-kw
     :type :fixed
     :size size}))

(defmethod make-edn-schema :array
  [schema-type name-kw items]
  {:type :array
   :items (u/ensure-edn-schema items)})

(defmethod make-edn-schema :map
  [schema-type name-kw values]
  {:type :map
   :values (u/ensure-edn-schema values)})

(defn get-unique-descriptor [schema]
  (if (satisfies? u/ILancasterSchema schema)
    (-> schema u/fingerprint64 u/long->str)
    (if (keyword? schema)
      schema
      (throw (ex-info (str "Unexpected schema type in union: `" schema "`.")
                      {:schema schema})))))

(defmethod make-edn-schema :union
  [schema-type name-kw member-schemas]
  (-> (reduce (fn [acc member-schema]
                (let [{:keys [descriptors edn-schema]} acc
                      descriptor (get-unique-descriptor member-schema)
                      member-edn-schema (u/ensure-edn-schema member-schema)]
                  (when (descriptors descriptor)
                    (throw
                     (ex-info "Identical schemas in union."
                              {:duplicated-schema-edn member-edn-schema
                               :descriptor descriptor})))
                  (-> acc
                      (update :descriptors conj descriptor)
                      (update :edn-schema conj member-edn-schema))))
              {:descriptors #{}
               :edn-schema []}
              member-schemas)
      :edn-schema))

(defn name-or-schema [edn-schema *names]
  (let [schema-name (u/edn-schema->name-kw edn-schema)]
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
     :enum (name-or-schema edn-schema *names)
     :fixed (name-or-schema edn-schema *names)
     :array (update edn-schema :items #(fix-repeated-schemas % *names))
     :map (update edn-schema :values #(fix-repeated-schemas % *names))
     :union (mapv #(fix-repeated-schemas % *names) edn-schema)
     :record (let [name-or-schema (name-or-schema edn-schema *names)
                   fix-field (fn [field]
                               (update field :type
                                       #(fix-repeated-schemas % *names)))]
               (if (map? name-or-schema)
                 (update edn-schema :fields #(mapv fix-field %))
                 name-or-schema))
     edn-schema)))

(defn edn-schema->lancaster-schema
  ([edn-schema*]
   (edn-schema->lancaster-schema edn-schema* nil))
  ([edn-schema* json-schema*]
   (when (= :name-keyword (u/get-avro-type edn-schema*))
     (throw (ex-info (str "Can't construct schema from name keyword: `"
                          edn-schema* "`. Must supply a full edn schema.")
                     {:given-edn-schema edn-schema*})))
   (let [name->edn-schema (u/make-name->edn-schema edn-schema*)
         edn-schema (u/ensure-defaults (fix-repeated-schemas edn-schema*)
                                       name->edn-schema)
         avro-schema (if (u/avro-primitive-types edn-schema)
                       (name edn-schema)
                       (u/edn-schema->avro-schema edn-schema))
         json-schema (or json-schema* (u/edn->json-string avro-schema))
         parsing-canonical-form (pcf-utils/avro-schema->pcf avro-schema)
         fingerprint64 (fingerprint/fingerprint64 parsing-canonical-form)
         plumatic-schema (u/edn-schema->plumatic-schema edn-schema
                                                        name->edn-schema)
         *name->serializer (u/make-initial-*name->f
                            #(u/make-serializer %1 name->edn-schema %2))
         *writer-fp->deserializer (atom {})
         serializer (u/make-serializer edn-schema name->edn-schema
                                       *name->serializer)
         default-data-size (u/make-default-data-size edn-schema
                                                     name->edn-schema)]
     (->LancasterSchema
      edn-schema name->edn-schema json-schema parsing-canonical-form
      fingerprint64 plumatic-schema serializer default-data-size
      *name->serializer *writer-fp->deserializer))))

(defn json-schema->lancaster-schema [json-schema]
  (let [edn-schema (-> json-schema
                       (u/json-schema->avro-schema)
                       (u/avro-schema->edn-schema))]
    (edn-schema->lancaster-schema edn-schema json-schema)))

(defn validate-name-kw [name-kw]
  (when-not (re-matches #"[A-Za-z][A-Za-z0-9\-]*" (name name-kw))
    (throw (ex-info
            (str "Name keywords must start with a letter and "
                 "subsequently may only contain letters, numbers, "
                 "or hyphens")
            {:given-name-kw name-kw}))))

(defn schema
  ([schema-type ns-name schema-name args]
   (let [name-kw (keyword ns-name schema-name)]
     (schema schema-type name-kw args)))
  ([schema-type name-kw args]
   (when (u/avro-named-types schema-type)
     (when (not (keyword? name-kw))
       (let [fn-name (str (name schema-type) "-schema")]
         (throw (ex-info (str "First arg to " fn-name " must be a name keyword."
                              "The keyword can be namespaced or not.")
                         {:given-name-kw name-kw}))))
     (validate-name-kw name-kw))
   (when-not (u/avro-primitive-types schema-type)
     (validate-schema-args schema-type args))
   (let [edn-schema (if (u/avro-primitive-types schema-type)
                      schema-type
                      (make-edn-schema schema-type name-kw args))]
     (edn-schema->lancaster-schema edn-schema))))

(defn primitive-schema [schema-kw]
  (schema schema-kw nil nil))

(defn schema-or-kw? [x]
  (or (instance? LancasterSchema x)
      (keyword? x)))

(defmethod validate-schema-args :record
  [schema-type fields]
  (when-not (sequential? fields)
    (throw (ex-info (str "Second arg to record-schema must be a sequence "
                         "of field definitions.")
                    {:given-fields fields})))
  (doseq [field fields]
    (when-not (sequential? field)
      (throw (ex-info (str "Second arg to record-schema must be a "
                           "sequence of field definitions.")
                      {:given-fields fields})))
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
          (u/serialize field-schema (impl/output-stream 100) default)
          (catch #?(:clj Exception :cljs js/Error) e
            (let [ex-msg (u/ex-msg e)]
              (if-not (str/includes? ex-msg "not a valid")
                (throw e)
                (throw
                 (ex-info
                  (str "Default value for field `" name-kw "` is invalid. "
                       ex-msg)
                  (u/sym-map name-kw default ex-msg))))))))))
  (let [dups (vec (for [[field-name freq] (frequencies (map first fields))
                        :when (> (int freq) 1)]
                    field-name))]
    (when (pos? (count dups))
      (throw
       (ex-info
        (str "Field names must be unique. Duplicated field-names: " dups)
        (u/sym-map dups))))))

(defmethod validate-schema-args :enum
  [schema-type symbols]
  (when-not (sequential? symbols)
    (throw (ex-info (str "Second arg to enum-schema must be a sequence "
                         "of keywords.")
                    {:given-symbols symbols})))
  (doseq [symbol symbols]
    (when-not (keyword? symbol)
      (throw (ex-info "All symbols in an enum must be keywords."
                      {:given-symbol symbol})))))

(defmethod validate-schema-args :fixed
  [schema-type size]
  (when-not (integer? size)
    (throw (ex-info (str "Second arg to fixed-schema (size) must be an "
                         "integer.")
                    {:given-size size}))))

(defmethod validate-schema-args :array
  [schema-type items-schema]
  (when-not (schema-or-kw? items-schema)
    (throw
     (ex-info (str "Arg to array-schema must be a schema object "
                   "or a name keyword.")
              {:given-items-schema items-schema}))))

(defmethod validate-schema-args :map
  [schema-type values-schema]
  (when-not (schema-or-kw? values-schema)
    (throw
     (ex-info (str "Arg to map-schema must be a schema object "
                   "or a name keyword.")
              {:given-values-schema values-schema}))))

(defmethod validate-schema-args :union
  [schema-type member-schemas]
  (when-not (sequential? member-schemas)
    (throw (ex-info (str "Arg to union-schema must be a sequence "
                         "of member schema objects or name keywords.")
                    {:given-member-edn-schemas member-schemas})))
  (doseq [member-schema member-schemas]
    (when-not (schema-or-kw? member-schema)
      (throw
       (ex-info (str "All member schemas in a union must be schema objects "
                     "or name keywords.")
                {:bad-member-schema member-schema}))))
  (let [schemas-to-check (reduce (fn [acc sch]
                                   (if-not (keyword? sch)
                                     (conj acc (u/edn-schema sch))
                                     (if (u/avro-primitive-types sch)
                                       (conj acc sch)
                                       acc)))
                                 [] member-schemas)]
    (when (u/contains-union? schemas-to-check)
      (throw (ex-info (str "Illegal union. Unions cannnot immediately contain "
                           "other unions.")
                      {:member-edn-schemas (map u/edn-schema member-schemas)})))
    (doseq [schema-type (set/union u/avro-primitive-types #{:map :array})]
      (when (u/more-than-one? #{schema-type} schemas-to-check)
        (throw (ex-info (str "Illegal union. Unions may not contain more than "
                             "one " (name schema-type) " schema.")
                        {:member-edn-schemas
                         (map u/edn-schema member-schemas)}))))
    (when (u/more-than-one? u/avro-numeric-types schemas-to-check)
      (throw (ex-info (str "Illegal union. Unions may not contain more than "
                           "one numeric schema (int, long, float, or double).")
                      {:member-edn-schemas (map u/edn-schema member-schemas)})))
    (when (u/more-than-one? u/avro-byte-types schemas-to-check)
      (throw (ex-info (str "Illegal union. Unions may not contain more than "
                           "one byte-array schema (bytes or fixed).")
                      {:member-edn-schemas
                       (map u/edn-schema member-schemas)})))))
