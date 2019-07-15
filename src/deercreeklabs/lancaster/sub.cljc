(ns deercreeklabs.lancaster.sub
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]))

(defmulti edn-sub-schemas u/avro-type-dispatch)

(defmethod edn-sub-schemas :default
  [edn-schema]
  [edn-schema])

(defmethod edn-sub-schemas :logical-type
  [edn-schema]
  (let [{:keys [edn-sub-schemas]} edn-schema]
    (cond-> [edn-schema]
      edn-sub-schemas (concat edn-sub-schemas))))

(defmethod edn-sub-schemas :record
  [edn-schema]
  (reduce (fn [acc {:keys [type]}]
            (let [sub-schemas (edn-sub-schemas type)]
              (concat acc sub-schemas)))
          [edn-schema] (:fields edn-schema)))

(defmethod edn-sub-schemas :array
  [edn-schema]
  (-> (:items edn-schema)
      (edn-sub-schemas)
      (conj edn-schema)))

(defmethod edn-sub-schemas :map
  [edn-schema]
  (-> (:values edn-schema)
      (edn-sub-schemas)
      (conj edn-schema)))

(defmethod edn-sub-schemas :union
  [edn-schema]
  (conj edn-schema edn-schema))

(defmulti expand-name-kws u/avro-type-dispatch)

(defmethod expand-name-kws :default
  [edn-schema name->edn-schema]
  edn-schema)

(defmethod expand-name-kws :name-keyword
  [edn-schema name->edn-schema]
  (name->edn-schema edn-schema))

(defmethod expand-name-kws :map
  [edn-schema name->edn-schema]
  (update edn-schema :values #(expand-name-kws % name->edn-schema)))

(defmethod expand-name-kws :array
  [edn-schema name->edn-schema]
  (update edn-schema :items #(expand-name-kws % name->edn-schema)))

(defn expand-fields [fields name->edn-schema]
  (mapv (fn [field]
          (update field :type #(expand-name-kws % name->edn-schema)))
        fields))

(defmethod expand-name-kws :record
  [edn-schema name->edn-schema]
  (update edn-schema :fields #(expand-fields % name->edn-schema) ))

(defmethod expand-name-kws :union
  [edn-schema name->edn-schema]
  (mapv #(expand-name-kws % name->edn-schema) edn-schema))

(defn dedupe-schemas [schemas]
  (vals (reduce (fn [acc schema]
                  (assoc acc (u/fingerprint64 schema) schema))
                {} schemas)))

(defn sub-schemas [schema]
  (let [top-edn-schema (u/edn-schema schema)]
    (if-not (u/avro-container-types (u/get-avro-type top-edn-schema))
      [schema]
      (let [name->edn-schema (u/make-name->edn-schema top-edn-schema)
            edn-subs (-> (edn-sub-schemas top-edn-schema)
                         (expand-name-kws name->edn-schema))]
        (->> (map schemas/edn-schema->lancaster-schema edn-subs)
             (dedupe-schemas))))))

(defmulti edn-schema-at-path u/avro-type-dispatch-lt)

(defmethod edn-schema-at-path :default
  [edn-schema full-path i]
  edn-schema)

(defmethod edn-schema-at-path :logical-type
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)
          {:keys [logical-type valid-k? k->child-edn-schema]} edn-schema]
      (if (and valid-k? k->child-edn-schema)
        (if (valid-k? k)
          (k->child-edn-schema k)
          (throw (ex-info (str "Key `" k "` is not a valid key for logical "
                               "type `" logical-type "`.")
                          (u/sym-map k full-path i edn-schema logical-type))))
        (throw (ex-info (str "Logical type `" logical-type "` does not support "
                             "children, but path points to children.")
                        (u/sym-map k full-path i edn-schema logical-type)))))))

(defmethod edn-schema-at-path :record
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)
          _ (when-not (keyword? k)
              (throw
               (ex-info (str "Path points to a record, but key `" k
                             "` is not a keyword.")
                        (u/sym-map full-path i k))))
          field (reduce (fn [acc field*]
                          (if (= k (:name field*))
                            (reduced field*)
                            acc))
                        nil (:fields edn-schema))]
      (when-not field
        (let [ex-type :not-a-field]
          (throw (ex-info (str "Key `" k "` is not a field of the indicated "
                               "record.")
                          (u/sym-map full-path i k edn-schema ex-type)))))
      (edn-schema-at-path (:type field) full-path (inc i)))))

(defmethod edn-schema-at-path :array
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (integer? k)
        (throw (ex-info (str "Path points to an array, but key `" k
                             "` is not an integer.")
                        (u/sym-map full-path i k))))
      (edn-schema-at-path (:items edn-schema) full-path (inc i)))))

(defmethod edn-schema-at-path :map
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (string? k)
        (throw (ex-info (str "Path points to a map, but key `" k
                             "` is not a string.")
                        (u/sym-map full-path i k))))
      (edn-schema-at-path (:values edn-schema) full-path (inc i)))))

(defmulti key-schema u/avro-type-dispatch-lt)

(defmethod key-schema :default
  [parent-edn-schema k]
  nil)

(defmethod key-schema :record
  [parent-edn-schema k]
  (when (keyword? k)
    (try
      (edn-schema-at-path parent-edn-schema [k] 0)
      (catch #?(:clj Exception :cljs js/Error) e
        (when-not (= :not-a-field (:ex-type (ex-data e)))
          (throw e))))))

(defmethod key-schema :array
  [parent-edn-schema k]
  (when (integer? k)
    (:items parent-edn-schema)))

(defn choose-member-edn-schema [union-edn-schema k]
  (let [edn-schema (reduce (fn [acc member-schema]
                             (if-let [s (key-schema member-schema k)]
                               (reduced s)
                               acc))
                           nil union-edn-schema)]
    (or edn-schema
        (throw (ex-info (str "No matching schema in union for key `" k "`.")
                        (u/sym-map union-edn-schema k))))))

(defmethod edn-schema-at-path :union
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (choose-member-edn-schema edn-schema (nth full-path i))))

(defn schema-at-path [schema path]
  (let [top-edn-schema (u/edn-schema schema)
        sub-edn-schema (edn-schema-at-path top-edn-schema path 0)
        avro-type (u/get-avro-type sub-edn-schema)
        sub-edn-schema* (if (not= :name-keyword avro-type)
                          sub-edn-schema
                          (-> (u/make-name->edn-schema top-edn-schema)
                              (get sub-edn-schema)))]
    (schemas/edn-schema->lancaster-schema sub-edn-schema*)))
