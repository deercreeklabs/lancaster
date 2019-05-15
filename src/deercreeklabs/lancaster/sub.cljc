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
            (set/union acc (edn-sub-schemas type)))
          #{edn-schema} (:fields edn-schema)))

(defmethod edn-sub-schemas :array
  [edn-schema]
  (->> (:items edn-schema)
       (edn-sub-schemas)
       (conj edn-schema)))

(defmethod edn-sub-schemas :map
  [edn-schema]
  (->> (:values edn-schema)
       (edn-sub-schemas)
       (conj edn-schema)))

(defmethod edn-sub-schemas :flex-map
  [edn-schema]
  (->> (:values edn-schema)
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
                          (if (= k (:name field*))
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

(defmethod edn-schema-at-path :union
  [edn-schema full-path i]
  (if (>= i (count full-path))
    edn-schema
    :foo))

(defn schema-at-path [schema path]
  (-> (u/edn-schema schema)
      (edn-schema-at-path path 0)
      (schemas/edn-schema->lancaster-schema)))
