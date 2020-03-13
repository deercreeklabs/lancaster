(ns deercreeklabs.lancaster.sub
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]))


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

(defmulti edn-schema-at-path u/avro-type-dispatch-lt)

(defmethod edn-schema-at-path :default
  [edn-schema full-path i name->edn-schema]
  edn-schema)

(defmethod edn-schema-at-path :logical-type
  [edn-schema full-path i name->edn-schema]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)
          {:keys [logical-type valid-k? k->child-edn-schema]} edn-schema]
      (if (and valid-k? k->child-edn-schema)
        (if (valid-k? k)
          (-> (k->child-edn-schema k)
              (expand-name-kws name->edn-schema))
          (throw (ex-info (str "Key `" k "` is not a valid key for logical "
                               "type `" logical-type "`.")
                          (u/sym-map k full-path i edn-schema logical-type))))
        (throw (ex-info (str "Logical type `" logical-type "` does not support "
                             "children, but path points to children.")
                        (u/sym-map k full-path i edn-schema logical-type)))))))

(defmethod edn-schema-at-path :record
  [edn-schema full-path i name->edn-schema]
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
      (when field
        (let [child (-> (:type field)
                        (expand-name-kws name->edn-schema))]
          (edn-schema-at-path child full-path (inc i) name->edn-schema))))))

(defmethod edn-schema-at-path :array
  [edn-schema full-path i name->edn-schema]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (integer? k)
        (throw (ex-info (str "Path points to an array, but key `" k
                             "` is not an integer.")
                        (u/sym-map full-path i k))))
      (let [child (-> (:items edn-schema)
                      (expand-name-kws name->edn-schema))]
        (edn-schema-at-path child full-path (inc i) name->edn-schema)))))

(defmethod edn-schema-at-path :map
  [edn-schema full-path i name->edn-schema]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (string? k)
        (throw (ex-info (str "Path points to a map, but key `" k
                             "` is not a string.")
                        (u/sym-map full-path i k))))
      (let [child (-> (:values edn-schema)
                      (expand-name-kws name->edn-schema))]
        (edn-schema-at-path child full-path (inc i) name->edn-schema)))))

(defmulti key-schema u/avro-type-dispatch-lt)

(defmethod key-schema :default
  [parent-edn-schema k name->edn-schema]
  nil)

(defmethod key-schema :record
  [parent-edn-schema k name->edn-schema]
  (when (keyword? k)
    (try
      (edn-schema-at-path parent-edn-schema [k] 0 name->edn-schema)
      (catch #?(:clj Exception :cljs js/Error) e
        (when-not (= :not-a-field (:ex-type (ex-data e)))
          (throw e))))))

(defmethod key-schema :array
  [parent-edn-schema k name->edn-schema]
  (when (integer? k)
    (-> (:items parent-edn-schema)
        (expand-name-kws name->edn-schema))))

(defmethod key-schema :map
  [parent-edn-schema k name->edn-schema]
  (when (string? k)
    (-> (:values parent-edn-schema)
        (expand-name-kws name->edn-schema))))

(defmethod edn-schema-at-path :union
  [edn-schema full-path i name->edn-schema]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)
          child (or (some #(key-schema % k name->edn-schema) edn-schema)
                    (throw (ex-info (str "No matching schema in union for key `"
                                         k "`.")
                                    (u/sym-map edn-schema k))))]
      (edn-schema-at-path child full-path (inc i) name->edn-schema))))

(defn schema-at-path [schema path]
  (let [top-edn-schema (u/edn-schema schema)
        name->edn-schema (u/make-name->edn-schema top-edn-schema)
        sub-edn-schema (edn-schema-at-path top-edn-schema path 0
                                           name->edn-schema)]
    (when sub-edn-schema
      (schemas/edn-schema->lancaster-schema sub-edn-schema))))
