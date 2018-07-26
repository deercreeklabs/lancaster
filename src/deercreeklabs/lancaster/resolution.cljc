(ns deercreeklabs.lancaster.resolution
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.pcf :as pcf]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

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

(defn flex-rec->flex-map [{:keys [ks vs]}]
  (zipmap ks vs))

(defmethod make-xf [:record :flex-map]
  [writer-edn-schema reader-edn-schema]
  (check-names writer-edn-schema reader-edn-schema)
  (let [reader-rec-schema (u/flex-map-edn-schema->record-edn-schema
                           reader-edn-schema)
        xf (make-xf writer-edn-schema reader-rec-schema)]
    (comp flex-rec->flex-map xf)))

(defmethod make-xf [:flex-map :flex-map]
  [writer-edn-schema reader-edn-schema]
  (check-names writer-edn-schema reader-edn-schema)
  (let [writer-rec-schema (u/flex-map-edn-schema->record-edn-schema
                           writer-edn-schema)
        reader-rec-schema (u/flex-map-edn-schema->record-edn-schema
                           reader-edn-schema)
        xf (make-xf writer-rec-schema reader-rec-schema)]
    (comp flex-rec->flex-map xf)))

(defmethod make-xf [:enum :enum]
  [writer-edn-schema reader-edn-schema]
  (check-names writer-edn-schema reader-edn-schema)
  (let [reader-symbol-set (set (:symbols reader-edn-schema))]
    (fn xf [written-symbol]
      (or (reader-symbol-set written-symbol)
          (throw
           (ex-info (str "Symbol " written-symbol " is not in the reader's "
                         "symbol list.")
                    {:reader-symbols (:symbols reader-edn-schema)
                     :written-symbol written-symbol}))))))

(defn fields->fields-map [fields]
  (reduce (fn [acc field]
            (assoc acc (:name field) field))
          {} fields))

(defn edn-schema->pcf [edn-schema]
  (-> edn-schema
      (u/edn-schema->avro-schema)
      (pcf/avro-schema->pcf)))

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
                  acc)))
            {:added-fields []
             :deleted-fields []
             :changed-fields []}
            all-field-names)))

(defmulti make-record-xf (fn [added-fields deleted-fields changed-fields]
                           (let [added? (pos? (count added-fields))
                                 deleted? (pos? (count deleted-fields))
                                 changed? (pos? (count changed-fields))]
                             [added? deleted? changed?])))

(defn make-add-fields-fn [added-fields]
  (let [new-fields (reduce (fn [acc {:keys [name default]}]
                             (-> acc
                                 (conj name)
                                 (conj default)))
                           [] added-fields)]
    (fn add-fields [r]
      (apply assoc r new-fields))))

(defn make-change-fields-fn [changed-fields]
  (let [field-name->xf (reduce (fn [acc [writer-field reader-field]]
                                 (let [{:keys [name]} writer-field
                                       xf (make-xf (:type writer-field)
                                                   (:type reader-field))]
                                   (assoc acc name xf)))
                               {} changed-fields)]
    (fn change-fields [r]
      (persistent!
       (reduce-kv (fn [acc field-name xf]
                    (assoc! acc field-name (xf (get acc field-name))))
                  (transient r) field-name->xf)))))

(defn make-delete-fields-fn [deleted-fields]
  (let [field-names (mapv :name deleted-fields)]
    (fn delete-fields [r]
      (apply dissoc r field-names))))

(defmethod make-record-xf [false false false]
  [added-fields deleted-fields changed-fields]
  identity)

(defmethod make-record-xf [false false true]
  [added-fields deleted-fields changed-fields]
  (make-change-fields-fn changed-fields))

(defmethod make-record-xf [false true false]
  [added-fields deleted-fields changed-fields]
  (make-delete-fields-fn deleted-fields))

(defmethod make-record-xf [false true true]
  [added-fields deleted-fields changed-fields]
  (let [delete-fields (make-delete-fields-fn deleted-fields)
        change-fields (make-change-fields-fn changed-fields)]
    (fn [r]
      (-> r
          (delete-fields)
          (change-fields)))))

(defmethod make-record-xf [true false false]
  [added-fields deleted-fields changed-fields]
  (make-add-fields-fn added-fields))

(defmethod make-record-xf [true false true]
  [added-fields deleted-fields changed-fields]
  (let [add-fields (make-add-fields-fn added-fields)
        change-fields (make-change-fields-fn changed-fields)]
    (fn [r]
      (-> r
          (change-fields)
          (add-fields)))))

(defmethod make-record-xf [true true false]
  [added-fields deleted-fields changed-fields]
  (let [add-fields (make-add-fields-fn added-fields)
        delete-fields (make-delete-fields-fn deleted-fields)]
    (fn [r]
      (-> r
          (delete-fields)
          (add-fields)))))

(defmethod make-record-xf [true true true]
  [added-fields deleted-fields changed-fields]
  (let [add-fields (make-add-fields-fn added-fields)
        delete-fields (make-delete-fields-fn deleted-fields)
        change-fields (make-change-fields-fn changed-fields)]
    (fn [r]
      (-> r
          (delete-fields)
          (change-fields)
          (add-fields)))))

(defmethod make-xf [:record :record]
  [writer-edn-schema reader-edn-schema]
  (check-names writer-edn-schema reader-edn-schema)
  (let [writer-fields (:fields writer-edn-schema)
        reader-fields (:fields reader-edn-schema)
        changes (get-field-changes writer-fields reader-fields)
        {:keys [added-fields deleted-fields changed-fields]} changes]
    (make-record-xf added-fields deleted-fields changed-fields)))

(defn throw-mismatch-error
  [writer-edn-schema reader-edn-schema]
  (let [writer-type (u/get-avro-type writer-edn-schema)
        reader-type (u/get-avro-type reader-edn-schema)]
    (throw
     (ex-info (str "Writer schema (" writer-type ") and reader schema ("
                   reader-type ") do not match.")
              (u/sym-map writer-edn-schema reader-edn-schema
                         writer-type reader-type)))))

(defn try-schema-pair [writer-item-schema reader-item-schema]
  (try
    [true (make-xf writer-item-schema reader-item-schema)]
    (catch #?(:clj Exception :cljs js/Error) e
      (let [msg (lu/get-exception-msg e)]
        (if-not (str/includes? msg "do not match.")
          (throw e)
          [false nil]
          )))))

(defn make-union-xf [writer-item-schema reader-union-schema]
  (loop [branch 0]
    (let [[success? xf] (try-schema-pair writer-item-schema
                                         (reader-union-schema branch))]
      (if success?
        xf
        (if (< (inc branch) (count reader-union-schema))
          (recur (inc branch))
          (fn [data]
            (throw-mismatch-error writer-item-schema reader-union-schema)))))))

(defn make-union-resolving-decoder
  [writer-edn-schema reader-edn-schema writer-type reader-type
   *name->deserializer]
  (cond
    (and (= :union writer-type) (= :union reader-type))
    (let [branch->deserializer (mapv
                                #(u/make-deserializer % *name->deserializer)
                                writer-edn-schema)
          branch->schema-name (mapv u/get-schema-name writer-edn-schema)
          branch->xf (mapv #(make-union-xf % (vec reader-edn-schema))
                           writer-edn-schema)]
      (if (u/wrapping-required? reader-edn-schema)
        (fn deserialize [is]
          (let [branch (u/read-long-varint-zz is)
                deserializer (branch->deserializer branch)
                xf (branch->xf branch)
                data (xf (deserializer is))
                schema-name (branch->schema-name branch)]
            [schema-name data]))
        (fn deserialize [is]
          (let [branch (u/read-long-varint-zz is)
                deserializer (branch->deserializer branch)
                xf (branch->xf branch)
                data (xf (deserializer is))]
            data))))

    (= :union writer-type)
    (let [branch->deserializer (mapv #(u/make-deserializer % *name->deserializer)
                                     writer-edn-schema)
          branch->schema-name (mapv u/get-schema-name writer-edn-schema)
          branch->xf (mapv #(make-union-xf % [reader-edn-schema])
                           writer-edn-schema)]
      (if (u/wrapping-required? reader-edn-schema)
        (fn deserialize [is]
          (let [branch (u/read-long-varint-zz is)
                deserializer (branch->deserializer branch)
                xf (branch->xf branch)
                data (xf (deserializer is))
                schema-name (branch->schema-name branch)]
            [schema-name data]))
        (fn deserialize [is]
          (let [branch (u/read-long-varint-zz is)
                deserializer (branch->deserializer branch)
                xf (branch->xf branch)
                data (xf (deserializer is))]
            data))))

    :else
    (let [xf (make-union-xf writer-edn-schema reader-edn-schema)
          deserializer (u/make-deserializer writer-edn-schema
                                            *name->deserializer)
          schema-name (u/get-schema-name writer-edn-schema)]
      (if (u/wrapping-required? reader-edn-schema)
        (fn deserialize [is]
          (let [data (xf (deserializer is))]
            [schema-name data]))
        (fn deserialize [is]
          (xf (deserializer is)))))))

(defn make-resolving-deserializer [writer-pcf reader-schema *name->deserializer]
  (let [writer-edn-schema (pcf/pcf->edn-schema writer-pcf)
        reader-edn-schema (u/get-edn-schema reader-schema)
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
        (let [msg (lu/get-exception-msg e)]
          (if (str/includes?
               msg
               #?(:clj "No method in multimethod 'make-xf'"
                  :cljs (str "No method in multimethod 'deercreeklabs."
                             "lancaster.resolution/make-xf'")))
            (throw-mismatch-error writer-edn-schema reader-edn-schema)
            (throw e)))))))
