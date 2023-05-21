(ns deercreeklabs.lancaster.schemas
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.deser :as deser]
   [deercreeklabs.lancaster.fingerprint :as fingerprint]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.pcf-utils :as pcf-utils]
   [deercreeklabs.lancaster.utils :as u]
   #?(:clj [clj-commons.primitive-math :as pm])
   [schema.core :as s :include-macros true]))

#?(:clj (pm/use-primitive-operators))

(declare edn-schema->lancaster-schema)

(def LancasterSchemaOrNameKW (s/if keyword?
                               s/Keyword
                               (s/protocol u/ILancasterSchema)))


(defn child-schema*
  ([edn-schema child-info]
   (let [{:lancaster/keys [schema-type
                           field->child-edn-schema
                           child-edn-schema]} child-info]
     (when (#{:record :union} schema-type)
       (throw (ex-info
               (str "Schema must not be record or union to call this arity "
                    "of `child-schema`")
               (u/sym-map edn-schema schema-type))))
     (if (= :logical-type schema-type)
       (let [child-edn (field->child-edn-schema)]
         (@u/*__INTERNAL__edn-schema->schema child-edn))
       (@u/*__INTERNAL__edn-schema->schema child-edn-schema))))
  ([edn-schema child-info field-or-branch]
   (let [{:lancaster/keys [branch->child-edn-schema
                           child-namespace
                           field->child-edn-schema
                           schema-type]} child-info]
     (when-not (#{:record :union :logical-type} schema-type)
       (throw (ex-info (str "Schema must be record or union to call this arity "
                            "of `child-schema`")
                       (u/sym-map edn-schema schema-type))))
     (cond
       (= :record schema-type)
       (let [field (if (or (qualified-keyword? field-or-branch)
                           (not child-namespace))
                     field-or-branch
                     (keyword (name child-namespace) (name field-or-branch)))
             child-edn (field->child-edn-schema field)]
         (@u/*__INTERNAL__edn-schema->schema child-edn))

       (= :logical-type schema-type)
       (let [child-edn (field->child-edn-schema field-or-branch)]
         (@u/*__INTERNAL__edn-schema->schema child-edn))

       (int? field-or-branch)
       (-> (nth branch->child-edn-schema field-or-branch)
           (@u/*__INTERNAL__edn-schema->schema))

       :else
       (some (fn [child-edn-schema]
               (let [child-schema* (@u/*__INTERNAL__edn-schema->schema
                                    child-edn-schema)
                     t (some-> child-schema* :child-info :lancaster/schema-type)]
                 (when (= :record t)
                   (u/child-schema child-schema* field-or-branch))))
             branch->child-edn-schema)))))

(defrecord LancasterSchema
    [edn-schema name->edn-schema json-schema parsing-canonical-form
     fingerprint64 fingerprint128 fingerprint256 plumatic-schema serializer
     default-data-size *name->serializer *writer-fp->deserializer child-info]
  u/ILancasterSchema
  (serialize [this data]
    (let [os (impl/output-stream default-data-size)]
      (u/serialize this os data)
      (u/to-byte-array os)))
  (serialize [this os data]
    (serializer os data []))
  (deserialize [this writer-schema is opts]
    (try
      (let [writer-fp (-> (u/fingerprint256 writer-schema)
                          (ba/byte-array->b64))
            deser (or (@*writer-fp->deserializer writer-fp)
                      (let [deser* (deser/make-deserializer
                                    (u/edn-schema writer-schema)
                                    edn-schema
                                    (:name->edn-schema writer-schema)
                                    name->edn-schema
                                    (atom {}))]
                        (swap! *writer-fp->deserializer assoc writer-fp deser*)
                        deser*))]
        (deser is opts))
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
  (fingerprint128 [this]
    fingerprint128)
  (fingerprint256 [this]
    fingerprint256)
  (plumatic-schema [this]
    plumatic-schema)
  (child-schema [this]
    (child-schema* edn-schema child-info))
  (child-schema [this field-or-branch]
    (child-schema* edn-schema child-info field-or-branch)))

(defmulti validate-schema-args u/first-arg-dispatch)
(defmulti make-edn-schema u/first-arg-dispatch)

(defn throw-bad-field-schema [field-name bad-schema field]
  (throw (ex-info (str "Bad field schema for field `" field-name
                       "`. Got `" bad-schema "`.")
                  (u/sym-map field-name bad-schema field))))

(defn throw-invalid-default [field-name schema bad-default]
  (let [field-edn-schema (u/edn-schema schema)]
    (throw (ex-info (str "Bad default value for field `" field-name
                         "`. Got `" bad-default "`.")
                    (u/sym-map field-name bad-default field-edn-schema)))))

(defn valid-default? [schema default]
  (try
    (u/serialize schema default)
    true
    (catch #?(:clj Exception :cljs js/Error) e
      false)))

(defn schema-or-kw? [x]
  (or (keyword? x)
      (:fingerprint64 x)))

(defn validate-name-kw [name-kw]
  (when-not (re-matches #"[A-Za-z][A-Za-z0-9\-]*" (name name-kw))
    (throw (ex-info
            (str "Name keywords must start with a letter and "
                 "subsequently may only contain letters, numbers, "
                 "or hyphens. Got `" name-kw "`.")
            {:given-name-kw name-kw}))))

(defn parse-field [field]
  (let [[field-name & more] field
        ret (u/sym-map field-name)]
    (when-not (keyword? field-name)
      (throw (ex-info (str "Field name must be a keyword. Got `"
                           field-name "`.")
                      {:given-field-name field-name
                       :field field})))
    (validate-name-kw field-name)
    (case (count more)
      0 (throw (ex-info (str "Missing field schema for field `" field-name "`.")
                        (u/sym-map field-name field)))
      1 (let [field-schema (first more)]
          (if (schema-or-kw? field-schema)
            (assoc ret :field-schema field-schema)
            (throw-bad-field-schema field-name field-schema field)))
      2 (let [[p0 p1] more]
          (if (string? p0)
            (if (schema-or-kw? p1)
              (assoc ret :doc p0 :field-schema p1)
              (throw-bad-field-schema field-name p1 field))
            (if (= :required p0)
              (if (schema-or-kw? p1)
                (assoc ret :required? true :field-schema p1)
                (throw-bad-field-schema field-name p1 field))
              (if (schema-or-kw? p0)
                (if (valid-default? p0 p1)
                  (assoc ret :field-schema p0 :default p1)
                  (throw-invalid-default field-name p0 p1))
                (throw-bad-field-schema field-name p0 field)))))
      3 (let [[p0 p1 p2] more]
          (if (string? p0)
            (if (= :required p1)
              (if (schema-or-kw? p2)
                (assoc ret :doc p0 :required? true :field-schema p2)
                (throw-bad-field-schema field-name p2 field))
              (if (schema-or-kw? p1)
                (if (valid-default? p1 p2)
                  (assoc ret :doc p0 :field-schema p1 :default p2)
                  (throw-invalid-default field-name p1 p2))
                (throw-bad-field-schema field-name p1 field)))
            (if (= :required p0)
              (if (schema-or-kw? p1)
                (if (valid-default? p1 p2)
                  (assoc ret :required? true :field-schema p1 :default p2)
                  (throw-invalid-default field-name p1 p2))
                (throw-bad-field-schema field-name p1 field))
              (throw-bad-field-schema field-name p0 field))))
      4 (let [[p0 p1 p2 p3] more]
          (if (and (string? p0)
                   (= :required p1)
                   (schema-or-kw? p2))
            (if (valid-default? p2 p3)
              (assoc ret :doc p0 :required? true :field-schema p2 :default p3)
              (throw-invalid-default field-name p2 p3))
            (throw-bad-field-schema field-name p2 field)))
      (throw (ex-info (str "Too many arguments (" (count more) ") in field `"
                           field-name "`.")
                      {:field-name field-name
                       :arguments more})))))

(defn make-record-field [field]
  ;; [field-name [docstring] [:required] field-schema [default]]
  (when-not (sequential? field)
    (throw (ex-info (str "Field must be a sequence. Got `" field "`.")
                    {:given-field field})))
  (let [params (parse-field field)
        {:keys [field-name doc required? field-schema default]} params
        _ (when-not (keyword? field-name)
            (throw (ex-info (str "Field names must be keywords. Bad field "
                                 "name: " field-name doc required?)
                            (u/sym-map field-name))))
        _ (when (qualified-keyword? field-name)
            (throw (ex-info (str "Field name keywords must not be namespaced. "
                                 "Bad field name: " field-name)
                            (u/sym-map field-name doc required?))))
        _ (when (u/fullname? field-name)
            (throw (ex-info (str "Field names must not contain dots. "
                                 "Bad field name: " field-name)
                            (u/sym-map field-name doc required?))))
        _ (when (and default (not required?))
            (throw (ex-info (str "Only :required fields may have a default. "
                                 "Field `" field-name "` has a default "
                                 "but is not :required.")
                            (u/sym-map field-name doc required?))))
        field-edn-schema (if (keyword? field-schema)
                           field-schema
                           (:edn-schema field-schema))
        field-type (u/get-avro-type field-edn-schema)
        ;; Make record fields optional unless required
        field-edn-schema* (cond
                            (or required?
                                (= :null field-type))
                            field-edn-schema

                            (= :union field-type)
                            (if (some #{:null} field-edn-schema)
                              field-edn-schema
                              (vec (cons :null field-edn-schema)))

                            :else
                            [:null field-edn-schema])]
    (cond-> {:name field-name
             :type field-edn-schema*
             :default (u/default-data field-edn-schema* default)}
      doc (assoc :doc doc))))

(defn check-field-dups [fields]
  (let [dups (vec (for [[field-name freq] (frequencies (map :name fields))
                        :when (> (int freq) 1)]
                    field-name))]
    (when (pos? (count dups))
      (throw
       (ex-info
        (str "Field names must be unique. Duplicated field-names: " dups)
        (u/sym-map dups))))))

(defmethod make-edn-schema :record
  ([schema-type name-kw fields]
   (make-edn-schema schema-type name-kw nil fields))
  ([schema-type name-kw docstring fields]
   (when-not (sequential? fields)
     (throw (ex-info "`fields` parameter be a sequence of field definitions"
                     {:given-fields fields})))
   (let [name-kw (u/qualify-name-kw name-kw {})
         fields* (binding [u/**enclosing-namespace** (or (namespace name-kw)
                                                         u/**enclosing-namespace**)]
                   (mapv make-record-field fields))]
     (check-field-dups fields*)
     (cond-> {:name name-kw
              :type :record
              :fields fields*}
       docstring (assoc :doc docstring)))))

(defmethod make-edn-schema :enum
  ([schema-type name-kw symbols*]
   (make-edn-schema schema-type name-kw nil symbols*))
  ([schema-type name-kw docstring symbols*]
   (let [symbols (cond
                   (vector symbols*) symbols*
                   (sequential? symbols*) (vec symbols*)
                   :else (throw (ex-info (str "Enum symbols list must be a "
                                              "sequence. Got `"
                                              (or symbols* "nil") "`.")
                                         {:symbols symbols*})))]
     (doseq [symbol symbols]
       (when (qualified-keyword? symbol)
         (throw (ex-info (str "Enum symbol keywords must not be namespaced. "
                              "Bad symbol keyword: " symbol)
                         (u/sym-map symbol symbols docstring)))))
     (let [name-kw (u/qualify-name-kw name-kw {})]
       (cond-> {:name name-kw
                :type :enum
                :symbols symbols
                :default (first symbols)}
         docstring (assoc :doc docstring))))))

(defmethod make-edn-schema :fixed
  [schema-type name-kw size]
  (let [name-kw (u/qualify-name-kw name-kw {})]
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
    (-> schema u/fingerprint256 ba/byte-array->b64)
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
                                       #(fix-repeated-schemas % *names)))
                   ns (or (namespace (:name edn-schema))
                          (:namespace edn-schema)
                          u/**enclosing-namespace**)]
               (binding [u/**enclosing-namespace** ns]
                 (if (map? name-or-schema)
                   (update edn-schema :fields #(mapv fix-field %))
                   name-or-schema)))
     edn-schema)))

(defn name-or-edn-schema [{:keys [*expanded-names edn-schema name->edn-schema]}]
  (let [schema-name (u/qualify-name-kw (u/named-edn-schema->name-kw edn-schema)
                                       edn-schema)]
    (if (@*expanded-names schema-name)
      schema-name
      (do
        (swap! *expanded-names conj schema-name)
        (if-not (keyword? edn-schema)
          edn-schema
          (or (name->edn-schema schema-name)
              (let [known-schema-names (sort (keys name->edn-schema))]
                (throw (ex-info
                        (str "No edn-schema found for name `"
                             schema-name "`.")
                        {:schema-name schema-name
                         :known-schema-names known-schema-names
                         :num-known-schema-names (count known-schema-names)
                         :edn-schema edn-schema})))))))))

(defn ensure-valid-edn-schema
  "Ensure that named schemas are expanded exactly once"
  [{:keys [edn-schema] :as arg}]
  (let [avro-type (u/get-avro-type edn-schema)
        ensure-child #(ensure-valid-edn-schema (assoc arg :edn-schema %))]
    (case avro-type
      :enum (name-or-edn-schema arg)
      :fixed (name-or-edn-schema arg)
      :array (update edn-schema :items ensure-child)
      :map (update edn-schema :values ensure-child)
      :union (mapv ensure-child edn-schema)
      :record (let [kw-or-schema (name-or-edn-schema arg)
                    fix-field (fn [field]
                                (update field :type ensure-child))]
                (if (map? kw-or-schema)
                  (update edn-schema :fields #(mapv fix-field %))
                  kw-or-schema))
      :name-keyword (name-or-edn-schema arg)
      edn-schema)))

(defn make-field->edn-schema [arg]
  (reduce (fn [acc field]
            (let [edn-schema (ensure-valid-edn-schema
                              (assoc arg
                                     :edn-schema (:type field)
                                     :*expanded-names (atom #{})))]
              (assoc acc (u/qualify-name-kw (:name field) edn-schema)
                     edn-schema)))
          {}
          (-> arg :edn-schema :fields)))

(defn ->child-info [{:keys [edn-schema] :as arg}]
  (let [name-kw (u/named-edn-schema->name-kw edn-schema)]
    (case (u/avro-type-dispatch edn-schema)
      :record
      {:lancaster/schema-type :record
       :lancaster/field->child-edn-schema (make-field->edn-schema
                                           (assoc arg :edn-schema edn-schema))
       :lancaster/child-namespace (or (namespace name-kw)
                                      (:namespace edn-schema)
                                      u/**enclosing-namespace**)}

      :map
      (let [child-edn-schema (ensure-valid-edn-schema
                              (assoc arg
                                     :edn-schema (:values edn-schema)
                                     :*expanded-names (atom #{})))]
        {:lancaster/schema-type :map
         :lancaster/child-edn-schema child-edn-schema})

      :array
      (let [child-edn-schema (ensure-valid-edn-schema
                              (assoc arg
                                     :edn-schema (:items edn-schema)
                                     :*expanded-names (atom #{})))]
        {:lancaster/schema-type :array
         :lancaster/child-edn-schema child-edn-schema})

      :union
      (let [branch->edn-schema (mapv #(ensure-valid-edn-schema
                                       (assoc arg
                                              :edn-schema %
                                              :*expanded-names (atom #{})))
                                     edn-schema)]
        {:lancaster/schema-type :union
         :lancaster/branch->child-edn-schema branch->edn-schema})

      :logical-type
      {:lancaster/schema-type :logical-type
       :lancaster/field->child-edn-schema (:k->child-edn-schema edn-schema)}

      ;; else
      {})))

(defn check-edn-schema [edn-schema]
  ;; TODO: Expand these checks
  (let [{ns* :namespace
         name* :name} edn-schema]
    (when (and ns* (not (string? ns*)))
      (throw (ex-info (str "If present, `:namespace` must be a string. Got `"
                           ns* "`.")
                      {:namespace ns*
                       :edn-schema edn-schema})))
    (when (and name* (not (keyword? name*)))
      (throw (ex-info (str "If present, `:name` must be a keyword. Got `"
                           name* "`.")
                      {:name name*
                       :edn-schema edn-schema})))))

(defn register-edn-schema! [{:keys [edn-schema] :as arg}]
  ;; This are necessary to prevent infinite recursion
  ;; They also prevent us from doing work that has already been done.
  (let [edn->schema @u/*__INTERNAL__edn-schema->schema]
    (when-not (edn->schema edn-schema)
      (let [lancaster-schema (edn-schema->lancaster-schema edn-schema)]
        (swap! u/*__INTERNAL__edn-schema->schema
               assoc edn-schema lancaster-schema)))))

(defn register-schema!
  [{:keys [child-info edn-schema lancaster-schema] :as arg}]
  (swap! u/*__INTERNAL__edn-schema->schema
         assoc edn-schema lancaster-schema)
  (let [{:lancaster/keys [branch->child-edn-schema
                          child-edn-schema
                          field->child-edn-schema
                          schema-type]} child-info
        child-edns (cond
                     (= :record schema-type)
                     (vals field->child-edn-schema)

                     (= :union schema-type)
                     branch->child-edn-schema

                     (#{:array :map} schema-type)
                     [child-edn-schema])]
    (doseq [child-edn child-edns]
      (let [edn->schema @u/*__INTERNAL__edn-schema->schema]
        (when-not (edn->schema child-edn)
          (let [child-lancaster-schema (edn-schema->lancaster-schema
                                        (assoc arg :edn-schema child-edn))]
            (swap! u/*__INTERNAL__edn-schema->schema
                   assoc child-edn child-lancaster-schema)))))))

(defn atom? [x]
  #?(:clj  (instance? clojure.lang.Atom x)
     :cljs (satisfies? IAtom x)))

(defn edn-schema->lancaster-schema
  [{:keys [*name->serializer edn-schema name->edn-schema json-schema] :as arg}]
  (when-not (atom? *name->serializer)
    (throw (ex-info (str "arg must contain a `:*name->serializer` atom.") {})))
  (when-not edn-schema
    (throw (ex-info (str "arg must contain an `:edn-schema`.") {})))
  (when-not (map? name->edn-schema)
    (throw (ex-info (str "arg must contain a `:name->edn-schema` map.") {})))
  (when (= :name-keyword (u/get-avro-type edn-schema))
    (throw (ex-info (str "Can't construct schema from name keyword: `"
                         edn-schema "`. Must supply a full edn schema.")
                    {:given-edn-schema edn-schema})))
  (check-edn-schema edn-schema)
  (let [edn-schema* (u/ensure-defaults (fix-repeated-schemas edn-schema)
                                       name->edn-schema)
        name-kw (u/named-edn-schema->name-kw edn-schema*)
        avro-schema (if (u/avro-primitive-types edn-schema*)
                      (name edn-schema*)
                      (u/edn-schema->avro-schema edn-schema*))
        json-schema* (or json-schema (u/edn->json-string avro-schema))
        parsing-canonical-form (pcf-utils/avro-schema->pcf avro-schema)
        fingerprint64 (fingerprint/fingerprint64 parsing-canonical-form)
        fingerprint128 (fingerprint/fingerprint128 parsing-canonical-form)
        fingerprint256 (fingerprint/fingerprint256 parsing-canonical-form)
        fp-str (ba/byte-array->b64 fingerprint128)
        plumatic-schema (u/edn-schema->plumatic-schema edn-schema*
                                                       name->edn-schema)
        *writer-fp->deserializer (atom {})
        serializer (u/make-serializer edn-schema* name->edn-schema
                                      *name->serializer)
        default-data-size (u/make-default-data-size edn-schema*
                                                    name->edn-schema)]
    (binding [u/**enclosing-namespace** (or (when name-kw
                                              (namespace name-kw))
                                            u/**enclosing-namespace**)]
      (let [child-info (->child-info (assoc arg :edn-schema edn-schema*))
            lancaster-schema (->LancasterSchema
                              edn-schema* name->edn-schema json-schema*
                              parsing-canonical-form fingerprint64
                              fingerprint128 fingerprint256 plumatic-schema
                              serializer default-data-size *name->serializer
                              *writer-fp->deserializer child-info)]
        (register-schema! (assoc arg
                                 :child-info child-info
                                 :edn-schema edn-schema*
                                 :lancaster-schema lancaster-schema))
        lancaster-schema))))

(defn schema
  ([schema-type name-kw args]
   (schema schema-type name-kw nil args))
  ([schema-type name-kw docstring args]
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
                      (if docstring
                        (make-edn-schema schema-type name-kw docstring args)
                        (make-edn-schema schema-type name-kw args)))]
     (edn-schema->lancaster-schema
      {:*name->serializer (atom {})
       :edn-schema edn-schema
       :name->edn-schema (u/make-name->edn-schema edn-schema)}))))

(defn primitive-schema [schema-kw]
  (schema schema-kw nil nil))

(defmethod validate-schema-args :record
  [schema-type fields]
  ;; Validation happens in make-edn-schema due to complex args
  nil)

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

(defn match? [reader-schema writer-schema]
  (when-not (satisfies? u/ILancasterSchema reader-schema))
  (try
    (deser/make-deserializer (u/edn-schema writer-schema)
                             (u/edn-schema reader-schema)
                             {} {} (atom {}))
    true
    (catch #?(:clj Exception :cljs js/Error) e
      (if-not (u/match-exception? e)
        (throw e)
        false))))

(defn maybe [sch]
  (let [schema-w-null #(schema :union nil [(primitive-schema :null) %])]
    (if (keyword? sch)
      (schema-w-null sch)
      (let [edn-schema (u/edn-schema sch)]
        (if (= :union (u/get-avro-type edn-schema))
          (if (some #{:null} edn-schema)
            sch
            (let [edn-schema* (vec (cons :null edn-schema))]
              (edn-schema->lancaster-schema
               {:*name->serializer (atom {})
                :edn-schema edn-schema*
                :name->edn-schema (u/make-name->edn-schema edn-schema*)})))
          (if (= :null edn-schema)
            sch
            (schema-w-null sch)))))))
