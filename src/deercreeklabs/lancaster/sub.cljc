(ns deercreeklabs.lancaster.sub
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]))

(defmulti edn-sub-schemas u/avro-type-dispatch)

(defmethod edn-sub-schemas :default
  [edn-schema]
  #{edn-schema})

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

(defmethod edn-sub-schemas :flex-map
  [edn-schema]
  (-> (:values edn-schema)
      (edn-sub-schemas)
      (conj edn-schema)))

(defmethod edn-sub-schemas :union
  [edn-schema]
  (-> (set edn-schema)
      (conj edn-schema)))

(defn sub-schemas [schema]
  (let [edn-schema (u/edn-schema schema)
        avro-type (u/get-avro-type edn-schema)]
    (if-not (u/avro-container-types avro-type)
      [schema]
      (->> (edn-sub-schemas edn-schema)
           (map schemas/edn-schema->lancaster-schema)))))


(defmulti edn-schema-at-path (fn [edn-schema & rest]
                               (u/get-avro-type edn-schema)))

(defmethod edn-schema-at-path :default
  [edn-schema full-path i]
  edn-schema)

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

(defmethod edn-schema-at-path :int-map
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (integer? k)
        (throw (ex-info (str "Path points to an int-map, but key `" k
                             "` is not a integer.")
                        (u/sym-map full-path i k))))
      (edn-schema-at-path (:values edn-schema) full-path (inc i)))))

(defmethod edn-schema-at-path :long-map
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (integer? k)
        (throw (ex-info (str "Path points to a long-map, but key `" k
                             "` is not a integer.")
                        (u/sym-map full-path i k))))
      (edn-schema-at-path (:values edn-schema) full-path (inc i)))))

(defmethod edn-schema-at-path :fixed-map
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    (let [k (nth full-path i)]
      (when-not (ba/byte-array? k)
        (throw (ex-info (str "Path points to a fixed-map, but key `" k
                             "` is not a byte array.")
                        (u/sym-map full-path i k))))
      (edn-schema-at-path (:values edn-schema) full-path (inc i)))))

(defmulti key-schema (fn [edn-schema & rest]
                       (u/get-avro-type edn-schema)))

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

(defmethod key-schema :int-map
  [parent-edn-schema k]
  (when (integer? k)
    (:values parent-edn-schema)))

(defmethod key-schema :long-map
  [parent-edn-schema k]
  (when (integer? k)
    (:values parent-edn-schema)))

(defmethod key-schema :map
  [parent-edn-schema k]
  (when (string? k)
    (:values parent-edn-schema)))

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
  (-> (u/edn-schema schema)
      (edn-schema-at-path path 0)
      (schemas/edn-schema->lancaster-schema)))
