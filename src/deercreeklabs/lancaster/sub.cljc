(ns deercreeklabs.lancaster.sub
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]))

; (declare expand-name-kws)

; (defn expand-name-kws-name-keyword [edn-schema #_name->edn-schema]
;   (let [schema (name->edn-schema edn-schema)]
;     (when-not schema
;       (throw (ex-info (str "Name keyword `" edn-schema "` is not found in "
;                            "this schema.")
;                       {:name-keyword edn-schema
;                        :known-names (keys name->edn-schema)})))
;     schema))

; (defn expand-name-kws-map [edn-schema name->edn-schema]
;   (update edn-schema :values #(expand-name-kws % name->edn-schema)))

; (defn expand-name-kws-array [edn-schema name->edn-schema]
;   (update edn-schema :items #(expand-name-kws % name->edn-schema)))

; (defn expand-fields [fields name->edn-schema]
;   (mapv (fn [field]
;           (update field :type #(expand-name-kws % name->edn-schema)))
;         fields))

; (defn expand-name-kws-record [edn-schema name->edn-schema]
;   (update edn-schema :fields #(expand-fields % name->edn-schema)))

; (defn expand-name-kws-union [edn-schema name->edn-schema]
;   (mapv #(expand-name-kws % name->edn-schema) edn-schema))

; (defn expand-name-kws [edn-schema name->edn-schema]
;   (let [avro-type (u/get-avro-type edn-schema)]
;     (case avro-type
;       :name-keyword (expand-name-kws-name-keyword edn-schema name->edn-schema)
;       :map (expand-name-kws-map edn-schema name->edn-schema)
;       :array (expand-name-kws-array edn-schema name->edn-schema)
;       :record (expand-name-kws-record edn-schema name->edn-schema)
;       :union (expand-name-kws-union edn-schema name->edn-schema)
;       edn-schema)))

; (defmulti edn-schema-at-path u/avro-type-dispatch-lt)

; (defmethod edn-schema-at-path :default
;   [edn-schema full-path i name->edn-schema]
;   edn-schema)

; (defmethod edn-schema-at-path :logical-type
;   [edn-schema full-path i name->edn-schema]
;   (if (>= i (count full-path))
;     edn-schema
;     (let [k (nth full-path i)
;           {:keys [logical-type valid-k? k->child-edn-schema]} edn-schema]
;       (if (and valid-k? k->child-edn-schema)
;         (if (valid-k? k)
;           (-> (k->child-edn-schema k)
;               (expand-name-kws name->edn-schema))
;           (throw (ex-info (str "Key `" k "` is not a valid key for logical "
;                                "type `" logical-type "`.")
;                           (u/sym-map k full-path i edn-schema logical-type))))
;         (throw (ex-info (str "Logical type `" logical-type "` does not support "
;                              "children, but path points to children.")
;                         (u/sym-map k full-path i edn-schema logical-type)))))))

; (defmethod edn-schema-at-path :record
;   [edn-schema full-path i name->edn-schema]
;   (if (>= i (count full-path))
;     edn-schema
;     (let [k (nth full-path i)
;           _ (when-not (keyword? k)
;               (throw
;                (ex-info (str "Path points to a record, but key `" k
;                              "` is not a keyword.")
;                         (u/sym-map full-path i k))))
;           field (reduce (fn [acc field*]
;                           (if (= k (:name field*))
;                             (reduced field*)
;                             acc))
;                         nil (:fields edn-schema))]
;       (when field
;         (let [child (-> (:type field)
;                         (expand-name-kws name->edn-schema))]
;           (edn-schema-at-path child full-path (inc i) name->edn-schema))))))

; (defmethod edn-schema-at-path :array
;   [edn-schema full-path i name->edn-schema]
;   (if (>= i (count full-path))
;     edn-schema
;     (let [k (nth full-path i)]
;       (when-not (integer? k)
;         (throw (ex-info (str "Path points to an array, but key `" k
;                              "` is not an integer.")
;                         (u/sym-map full-path i k))))
;       (let [child (-> (:items edn-schema)
;                       (expand-name-kws name->edn-schema))]
;         (edn-schema-at-path child full-path (inc i) name->edn-schema)))))

; (defmethod edn-schema-at-path :map
;   [edn-schema full-path i name->edn-schema]
;   (if (>= i (count full-path))
;     edn-schema
;     (let [k (nth full-path i)]
;       (when-not (string? k)
;         (throw (ex-info (str "Path points to a map, but key `" k
;                              "` is not a string.")
;                         (u/sym-map full-path i k))))
;       (let [child (-> (:values edn-schema)
;                       (expand-name-kws name->edn-schema))]
;         (edn-schema-at-path child full-path (inc i) name->edn-schema)))))

; (defmulti key-schema u/avro-type-dispatch-lt)

; (defmethod key-schema :default
;   [parent-edn-schema k name->edn-schema]
;   nil)

; (defmethod key-schema :record
;   [parent-edn-schema k name->edn-schema]
;   (when (keyword? k)
;     (try
;       (edn-schema-at-path parent-edn-schema [k] 0 name->edn-schema)
;       (catch #?(:clj Exception :cljs js/Error) e
;         (when-not (= :not-a-field (:ex-type (ex-data e)))
;           (throw e))))))

; (defmethod key-schema :array
;   [parent-edn-schema k name->edn-schema]
;   (when (integer? k)
;     (-> (:items parent-edn-schema)
;         (expand-name-kws name->edn-schema))))

; (defmethod key-schema :map
;   [parent-edn-schema k name->edn-schema]
;   (when (string? k)
;     (-> (:values parent-edn-schema)
;         (expand-name-kws name->edn-schema))))

; (defmethod edn-schema-at-path :union
;   [edn-schema full-path i name->edn-schema]
;   (if (>= i (count full-path))
;     edn-schema
;     (let [k (nth full-path i)
;           child (or (some #(key-schema % k name->edn-schema) edn-schema)
;                     (throw (ex-info (str "No matching schema in union for key `"
;                                          k "`.")
;                                     (u/sym-map edn-schema k))))]
;       (edn-schema-at-path child full-path (inc i) name->edn-schema))))

(defn path-type* [path-entry]
  (cond
   (keyword? path-entry) :keyword
   (string? path-entry) :string
   (int? path-entry) :int
   :else nil))

(defn ->matching-union-child-schema [schema edn-schema path-entry]
  (let [path-type (path-type* path-entry)
        ret (some
                (fn [[i sub]]
                  (let [avro-type (u/avro-type-dispatch-lt sub)]
                    (case [avro-type path-type]
                      [:record :keyword] (-> (u/child-schema schema i)
                                             (u/child-schema path-entry))
                      [:map :string] (-> (u/child-schema schema i)
                                         (u/child-schema))
                      [:array :int] (-> (u/child-schema schema i)
                                        (u/child-schema))
                      ;; Can't be union as per Avro spec disallowing
                      ;; immediately nested unions.
                      (if (= :logical-type avro-type)
                        (when-let [->ces (:k->child-edn-schema edn-schema)]
                          (-> (u/child-schema schema i)
                              (u/child-schema)))
                        nil))))
                (map-indexed vector edn-schema))]
    (if ret
      ret
      (throw
       (ex-info
        (str "No matching schema in union for "
             "key `" path-entry "`.")
        {:schema schema :k path-entry})))))

(defn schema-at-path [schema path]
  (let [child (first path)]
    (if-not child
      schema
      (let [edn-schema (:edn-schema schema)
            child-schema (case (u/avro-type-dispatch-lt edn-schema)
                           :record (u/child-schema schema child)
                           :map (u/child-schema schema)
                           :array (u/child-schema schema)
                           :union (->matching-union-child-schema
                                   schema edn-schema child)
                           :logical-type (u/child-schema schema)
                           (throw
                            (ex-info
                             "Can't get schema at path for non container type."
                             (u/sym-map schema path))))]
        (recur child-schema (rest path))))))

(defn member-schemas [schema]
  (let [{:keys [edn-schema #_name->edn-schema]} schema
        avro-type (u/get-avro-type edn-schema)
        _ (when-not (= :union avro-type)
            (throw (ex-info (str "The argument to `member-schemas` must be "
                                 "a union schema. Got type `" avro-type "`.")
                            (u/sym-map schema avro-type))))]
    (map (fn [sub] (if (keyword? sub)
                     (sub @u/*__INTERNAL__name->schema)
                     sub))
         (:child-info schema))))

(defn member-schema-at-branch [schema branch-index]
  (let [{:keys [edn-schema #_name->edn-schema]} schema
        avro-type (u/get-avro-type edn-schema)
        _ (when-not (= :union avro-type)
            (throw
             (ex-info (str "The `schema` argument to `member-schema-at-branch` "
                           "must be a union schema. Got type: `" avro-type "`.")
                      (u/sym-map edn-schema avro-type))))
        _ (when-not (int? branch-index)
            (throw
             (ex-info
              (str "The `branch-index` argument to `member-schema-at-branch` "
                   "must be an integer. Got: `" branch-index "`.")
              (u/sym-map branch-index edn-schema avro-type))))
        _ (when (or (>= branch-index (count edn-schema))
                    (neg? branch-index))
            (throw
             (ex-info
              (str "The `branch-index` argument (`" branch-index "`) "
                   "to `member-schema-at-branch` is out of range. Length of "
                   "union schema is " (count edn-schema) ".")
              (u/sym-map edn-schema branch-index))))
        member-edn-schema (nth edn-schema branch-index)]
    (u/child-schema schema branch-index)))
