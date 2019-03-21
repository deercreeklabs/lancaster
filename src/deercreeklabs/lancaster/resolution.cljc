(ns deercreeklabs.lancaster.resolution
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.pcf-utils :as pcf-utils]
   [deercreeklabs.lancaster.utils :as u]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]))

#?(:clj (pm/use-primitive-operators))

(defn check-names [writer-edn-schema reader-edn-schema]
  (let [writer-name (:name writer-edn-schema)
        reader-name (:name reader-edn-schema)]
    (if (= writer-name reader-name)
      true
      (throw
       (ex-info (str "Schema names do not match. (" writer-name " != "
                     reader-name ")")
                (u/sym-map writer-name reader-name))))))

(defmulti make-xf (fn [writer-edn-schema reader-edn-schema & args]
                    (let [writer-type (u/get-avro-type writer-edn-schema)
                          reader-type (u/get-avro-type reader-edn-schema)]
                      [writer-type reader-type])))

(defmethod make-xf [:int :long]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (u/int->long data)))

(defmethod make-xf [:int :float]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (float data)))

(defmethod make-xf [:int :double]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (double data)))

(defmethod make-xf [:long :float]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    #?(:clj (float data)
       :cljs
       (.toNumber data))))

(defmethod make-xf [:long :double]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    #?(:clj (double data)
       :cljs (.toNumber data))))

(defmethod make-xf [:float :double]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (double data)))

(defmethod make-xf [:string :bytes]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (ba/utf8->byte-array data)))

(defmethod make-xf [:bytes :string]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (ba/byte-array->utf8 data)))

(defmethod make-xf [:array :array]
  [writer-edn-schema reader-edn-schema]
  (let [xf-item (make-xf (:items writer-edn-schema)
                         (:items reader-edn-schema))]
    (fn xf [data]
      (map xf-item data))))

(defmethod make-xf [:map :map]
  [writer-edn-schema reader-edn-schema]
  (let [xf-value (make-xf (:values writer-edn-schema)
                          (:values reader-edn-schema))]
    (fn xf [data]
      (persistent!
       (reduce-kv (fn [acc k v]
                    (assoc! acc k (xf-value v)))
                  (transient {}) data)))))

(defn make-xf-flex-map [writer-edn-schema reader-edn-schema]
  (let [v-xf (make-xf (:values writer-edn-schema) (:values reader-edn-schema))]
    (fn xf [m]
      (persistent!
       (reduce-kv (fn [acc k v]
                    (assoc! acc k (v-xf v)))
                  (transient {}) m)))))

(defmethod make-xf [:int-map :int-map]
  [writer-edn-schema reader-edn-schema]
  (make-xf-flex-map writer-edn-schema reader-edn-schema))

(defmethod make-xf [:long-map :long-map]
  [writer-edn-schema reader-edn-schema]
  (make-xf-flex-map writer-edn-schema reader-edn-schema))

(defmethod make-xf [:fixed-map :fixed-map]
  [writer-edn-schema reader-edn-schema]
  (make-xf-flex-map writer-edn-schema reader-edn-schema))

(defmethod make-xf [:int-map :long-map]
  [writer-edn-schema reader-edn-schema]
  (let [writer-values-schema (:values writer-edn-schema)
        reader-values-schema (:values reader-edn-schema)
        v-xf (if (= writer-values-schema reader-values-schema)
               identity
               (make-xf writer-values-schema reader-values-schema))]
    (fn xf [m]
      (persistent!
       (reduce-kv (fn [acc k v]
                    (assoc! acc (u/int->long k) (v-xf v)))
                  (transient {}) m)))))

(defmethod make-xf [:enum :enum]
  [writer-edn-schema reader-edn-schema]
  (check-names writer-edn-schema reader-edn-schema)
  (let [{writer-key-ns-type :key-ns-type
         writer-name :name
         writer-symbols :symbols} writer-edn-schema
        {reader-key-ns-type :key-ns-type
         reader-name :name
         reader-symbols :symbols} reader-edn-schema
        ws->rs (reduce
                (fn [acc s]
                  (assoc acc
                         (u/ns-k writer-key-ns-type writer-name s)
                         (u/ns-k reader-key-ns-type reader-name s)))
                {} writer-symbols)]
    (fn xf [written-symbol]
      (or (ws->rs written-symbol)
          (throw
           (ex-info (str "Symbol " written-symbol " is not in the reader's "
                         "symbol list.")
                    {:written-symbol written-symbol
                     :reader-symbols (vals ws->rs)}))))))

(defn fields->fields-map [fields]
  (reduce (fn [acc field]
            (assoc acc (:name field) field))
          {} fields))

(defn edn-schema->pcf [edn-schema]
  (-> edn-schema
      (u/edn-schema->avro-schema)
      (pcf-utils/avro-schema->pcf)))

(defn equivalent-schemas? [edn-schema1 edn-schema2]
  (= (edn-schema->pcf edn-schema1)
     (edn-schema->pcf edn-schema2)))

(defn get-field-changes [writer-fields reader-fields]
  (let [writer-fields-map (fields->fields-map writer-fields)
        reader-fields-map (fields->fields-map reader-fields)
        writer-field-names (set (map :name writer-fields))
        reader-field-names (set (map :name reader-fields))
        all-field-names (set/union writer-field-names reader-field-names)]
    (reduce (fn [acc field-name]
              (let [writer-field (writer-fields-map field-name)
                    reader-field (reader-fields-map field-name)]
                (cond
                  (and writer-field (not reader-field))
                  (update acc :deleted-fields conj writer-field)

                  (and reader-field (not writer-field))
                  (update acc :added-fields conj reader-field)

                  (not (equivalent-schemas? (:type reader-field)
                                            (:type writer-field)))
                  (update acc :changed-fields conj
                          [writer-field reader-field])

                  :else
                  (update acc :same-fields conj writer-field))))
            {:added-fields []
             :deleted-fields []
             :changed-fields []
             :same-fields []}
            all-field-names)))

(defn make-add-fields-fn [reader-key-ns-fn added-fields]
  (let [new-fields (reduce (fn [acc {:keys [name default]}]
                             (-> acc
                                 (conj (reader-key-ns-fn name))
                                 (conj default)))
                           [] added-fields)]
    (fn add-fields [r]
      (apply assoc r new-fields))))

(defn make-change-fields-fn [reader-key-ns-fn changed-fields]
  (let [field-name->xf (reduce (fn [acc [writer-field reader-field]]
                                 (let [{:keys [name]} writer-field
                                       xf (make-xf (:type writer-field)
                                                   (:type reader-field))]
                                   (assoc acc (reader-key-ns-fn name) xf)))
                               {} changed-fields)]
    (fn change-fields [r]
      (persistent!
       (reduce-kv (fn [acc field-name xf]
                    (assoc! acc field-name (xf (get acc field-name))))
                  (transient r) field-name->xf)))))

(defn make-delete-fields-fn [writer-key-ns-fn deleted-fields]
  (let [field-names (mapv #(writer-key-ns-fn (:name %)) deleted-fields)]
    (fn delete-fields [r]
      (apply dissoc r field-names))))

(defn make-ns-same-fields [writer-key-ns-fn reader-key-ns-fn same-fields]
  (fn ns-fields [r]
    (persistent!
     (reduce (fn [acc {field-name :name}]
               (let [writer-key (writer-key-ns-fn field-name)]
                 (-> acc
                     (assoc! (reader-key-ns-fn field-name) (acc writer-key))
                     (dissoc! writer-key))))
             (transient r) same-fields))))

(defmethod make-xf [:record :record]
  [writer-edn-schema reader-edn-schema]
  (check-names writer-edn-schema reader-edn-schema)
  (let [{writer-fields :fields
         writer-key-ns-type :key-ns-type} writer-edn-schema
        {reader-fields :fields
         reader-key-ns-type :key-ns-type} reader-edn-schema
        changes (get-field-changes writer-fields reader-fields)
        {:keys [added-fields deleted-fields changed-fields same-fields]} changes
        writer-key-ns-fn (partial u/ns-k writer-key-ns-type
                                  (:name writer-edn-schema))
        reader-key-ns-fn (partial u/ns-k (:key-ns-type reader-edn-schema)
                                  (:name reader-edn-schema))
        add-fields (make-add-fields-fn reader-key-ns-fn added-fields)
        delete-fields (make-delete-fields-fn writer-key-ns-fn deleted-fields)
        change-fields (make-change-fields-fn reader-key-ns-fn changed-fields)
        diff-key-ns-types? (not= writer-key-ns-type reader-key-ns-type)
        ns-same-fields (make-ns-same-fields writer-key-ns-fn reader-key-ns-fn
                                            same-fields)]
    (fn [r]
      (cond-> r
        (seq deleted-fields) (delete-fields)
        (seq changed-fields) (change-fields)
        (seq added-fields) (add-fields)
        (and diff-key-ns-types? (seq same-fields)) (ns-same-fields)))))

(defn throw-mismatch-error
  [writer-edn-schema reader-edn-schema]
  (let [writer-type (u/get-avro-type writer-edn-schema)
        reader-type (u/get-avro-type reader-edn-schema)]
    (throw
     (ex-info (str "Writer schema (" writer-type ") and reader schema ("
                   reader-type ") do not match.")
              (u/sym-map writer-edn-schema reader-edn-schema
                         writer-type reader-type)))))

(defn make-xf* [writer-item-schema reader-item-schema]
  (try
    (make-xf writer-item-schema reader-item-schema)
    (catch #?(:clj Exception :cljs js/Error) e
      (when-not (str/includes? (u/ex-msg e) "do not match.")
        (throw e)))))

(defn make-union-xf [writer-item-schema reader-union-schema]
  (loop [branch 0]
    (if-let [xf (make-xf* writer-item-schema (reader-union-schema branch))]
      (if-let [metadata (u/branch-meta branch (reader-union-schema branch))]
        (fn [data]
          (with-meta (xf data) metadata))
        xf)
      (if (< (inc branch) (count reader-union-schema))
        (recur (inc branch))
        (fn [data]
          (throw-mismatch-error writer-item-schema reader-union-schema))))))

(defn make-union-resolving-decoder
  [writer-edn-schema reader-edn-schema writer-type reader-type
   *name->deserializer]
  (cond
    (and (= :union writer-type) (= :union reader-type))
    (let [branch->deserializer (mapv
                                #(u/make-deserializer % *name->deserializer)
                                writer-edn-schema)
          branch->xf (mapv #(make-union-xf % (vec reader-edn-schema))
                           writer-edn-schema)
          branch->metadata (vec (map-indexed u/branch-meta reader-edn-schema))]
      (fn deserialize [is]
        (let [branch (u/read-long-varint-zz is)
              deserializer (branch->deserializer branch)
              xf (branch->xf branch)]
          (xf (deserializer is)))))

    (= :union writer-type)
    (let [branch->deserializer (mapv #(u/make-deserializer
                                       % *name->deserializer)
                                     writer-edn-schema)
          branch->xf (mapv #(make-union-xf % [reader-edn-schema])
                           writer-edn-schema)]
      (fn deserialize [is]
        (let [branch (u/read-long-varint-zz is)
              deserializer (branch->deserializer branch)
              xf (branch->xf branch)]
          (xf (deserializer is)))))

    :else
    (let [xf (make-union-xf writer-edn-schema reader-edn-schema)
          deserializer (u/make-deserializer writer-edn-schema
                                            *name->deserializer)]
      (fn deserialize [is]
        (xf (deserializer is))))))

(defn resolving-deserializer
  [writer-edn-schema reader-schema *name->deserializer]
  (let [reader-edn-schema (u/edn-schema reader-schema)
        writer-type (u/get-avro-type writer-edn-schema)
        reader-type (u/get-avro-type reader-edn-schema)]
    (try
      (if (or (= :union writer-type) (= :union reader-type))
        (make-union-resolving-decoder writer-edn-schema reader-edn-schema
                                      writer-type reader-type
                                      *name->deserializer)
        (let [writer-deserializer (u/make-deserializer writer-edn-schema
                                                       *name->deserializer)
              xf (make-xf writer-edn-schema reader-edn-schema)]
          (fn deserialize [is]
            (xf (writer-deserializer is)))))
      (catch #?(:clj IllegalArgumentException :cljs js/Error) e
        (let [msg (u/ex-msg e)]
          (if (str/includes?
               msg
               #?(:clj "No method in multimethod 'make-xf'"
                  :cljs (str "No method in multimethod 'deercreeklabs."
                             "lancaster.resolution/make-xf'")))
            (throw-mismatch-error writer-edn-schema reader-edn-schema)
            (throw e)))))))
