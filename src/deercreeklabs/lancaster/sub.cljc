(ns deercreeklabs.lancaster.sub
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]))

(defmulti edn-sub-schemas u/avro-type-dispatch-lt)

(defmethod edn-sub-schemas :default
  [edn-schema]
  #{edn-schema})

(defmethod edn-sub-schemas :logical-type
  [edn-schema]
  (let [{:keys [edn-sub-schemas]} edn-schema]
    (cond->  #{edn-schema}
      edn-sub-schemas (set/union edn-sub-schemas))))

(defmethod edn-sub-schemas :record
  [edn-schema]
  (reduce (fn [acc {:keys [type]}]
            (let [sub-schemas (edn-sub-schemas type)]
              (set/union acc sub-schemas)))
          #{edn-schema} (:fields edn-schema)))

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
  (-> (set edn-schema)
      (conj edn-schema)))

(defn sub-schemas [schema]
  (let [top-edn-schema (u/edn-schema schema)]
    (if-not (u/avro-container-types (u/get-avro-type top-edn-schema))
      [schema]
      (map (fn [edn-sub-schema]
             (let [es (if (not= :name-keyword (u/get-avro-type edn-sub-schema))
                        edn-sub-schema
                        (-> (u/make-name->edn-schema top-edn-schema)
                            (get edn-sub-schema)))]
               (schemas/edn-schema->lancaster-schema es)))
           (edn-sub-schemas top-edn-schema)))))

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
                          (if (= (name k) (name (:name field*)))
                            (reduced field*)
                            acc))
                        nil (:fields edn-schema))]
      (when-not field
        (throw (ex-info (str "Key `" k "` is not a field of the indicated "
                             "record.")
                        (u/sym-map full-path i k edn-schema))))
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

(defn throw-unq-k-in-schema-at-path [edn-schema]
  (throw (ex-info (str "Only records with qualified keys may be used inside"
                       "unions in schema-at-path. Got: `" edn-schema "`.")
                  (u/sym-map edn-schema))))

(defmethod key-schema :record
  [parent-edn-schema k]
  (when (keyword? k)
    (let [{:keys [key-ns-type]} parent-edn-schema
          k-ns (namespace k)]
      (when-not k-ns
        (throw (ex-info (str "Only qualified keys may be used inside unions "
                             "in schema-at-path. Got: `" k "`.")
                        (u/sym-map parent-edn-schema k))))
      (case key-ns-type
        nil (throw-unq-k-in-schema-at-path parent-edn-schema)
        :none (throw-unq-k-in-schema-at-path parent-edn-schema)
        :short (let [rec-name (name (:name parent-edn-schema))]
                 (when (= rec-name k-ns)
                   (edn-schema-at-path parent-edn-schema [k] 0)))
        :fq (let [name-kw (:name parent-edn-schema)
                  rec-name (str (namespace name-kw) "." (name name-kw))]
              (when (= rec-name k-ns)
                (edn-schema-at-path parent-edn-schema [k] 0)))))))

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
        path-edn-schema (edn-schema-at-path top-edn-schema path 0)
        avro-type (u/get-avro-type path-edn-schema)
        sub-edn-schema (if (not= :name-keyword avro-type)
                         path-edn-schema
                         (-> (u/make-name->edn-schema top-edn-schema)
                             (get path-edn-schema)))]
    (schemas/edn-schema->lancaster-schema sub-edn-schema)))
