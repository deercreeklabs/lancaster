(ns deercreeklabs.lancaster.deser
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm]))
  #?(:cljs
     (:import
      (goog.math Long))))

(defn check-names [writer-edn-schema reader-edn-schema]
  (let [writer-name (:name writer-edn-schema)
        reader-name (:name reader-edn-schema)]
    (when (not= writer-name reader-name)
      (throw (ex-info (str "Schema names do not match. (" writer-name " != "
                           reader-name ")")
                      (u/sym-map writer-name reader-name))))))

(defmulti make-deserializer
  (fn [writer-edn-schema reader-edn-schema & _]
    (let [writer-type (u/get-avro-type writer-edn-schema)
          reader-type (u/get-avro-type reader-edn-schema)]
      (cond
        (:logical-type reader-edn-schema)
        :logical-type

        (and (= :union writer-type) (not= :union reader-type))
        [:union :other]

        (and (not= :union writer-type) (= :union reader-type))
        [:other :union]

        (and (= :name-keyword writer-type) (not= :name-keyword reader-type))
        [:name-keyword :other]

        (and (not= :name-keyword writer-type) (= :name-keyword reader-type))
        [:other :name-keyword]

        :else
        [writer-type reader-type]))))

(defmethod make-deserializer [:null :null]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    nil))

(defmethod make-deserializer [:boolean :boolean]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (= 1 (u/read-byte is))))

(defmethod make-deserializer [:int :int]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (int (u/read-long-varint-zz is))))

(defmethod make-deserializer [:int :long]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (u/read-long-varint-zz is)))

(defmethod make-deserializer [:int :float]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (float (u/read-long-varint-zz is))))

(defmethod make-deserializer [:int :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (double (u/read-long-varint-zz is))))

(defmethod make-deserializer [:long :long]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (u/read-long-varint-zz is)))

(defmethod make-deserializer [:long :float]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (let [l (u/read-long-varint-zz is)]
      #?(:clj (float l)
         :cljs (if (number? l)
                 l
                 (.toNumber ^Long l))))))

(defmethod make-deserializer [:long :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (let [l (u/read-long-varint-zz is)]
      #?(:clj (double l)
         :cljs (if (number? l)
                 l
                 (.toNumber ^Long l))))))

(defmethod make-deserializer [:float :float]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (u/read-float is)))

(defmethod make-deserializer [:float :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (double (u/read-float is))))

(defmethod make-deserializer [:double :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (u/read-double is)))

(defmethod make-deserializer [:string :string]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (u/read-utf8-string is)))

(defmethod make-deserializer [:string :bytes]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (ba/utf8->byte-array (u/read-utf8-string is))))

(defmethod make-deserializer [:bytes :bytes]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (u/read-len-prefixed-bytes is)))

(defmethod make-deserializer [:bytes :string]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is opts]
    (ba/byte-array->utf8 (u/read-len-prefixed-bytes is))))

(defmethod make-deserializer [:enum :enum]
  [writer-edn-schema reader-edn-schema writer-name->edn-schema
   reader-name->edn-schema *deserializers]
  (check-names writer-edn-schema reader-edn-schema)
  (let [writer-syms (:symbols writer-edn-schema)
        {:keys [symbols default name]} reader-edn-schema
        find-reader-symbol (fn [wsym]
                             (reduce (fn [acc rsym]
                                       (if (= wsym rsym)
                                         (reduced rsym)
                                         acc))
                                     default symbols))
        i->reader-symbol (mapv find-reader-symbol writer-syms)
        deserializer (fn deserialize [is opts]
                       (let [i (u/read-long-varint-zz is)]
                         (or (i->reader-symbol i)
                             (let [written-symbol (nth writer-syms i)]
                               (throw
                                (ex-info
                                 (str "Enum read error. Writer wrote `"
                                      written-symbol "` which does not exist "
                                      "in reader schema's symbols list, and "
                                      "reader schema does not have a default "
                                      "symbol.")
                                 (u/sym-map writer-edn-schema reader-edn-schema
                                            written-symbol)))))))]
    (swap! *deserializers assoc [writer-edn-schema reader-edn-schema]
           deserializer)
    deserializer))

(defmethod make-deserializer [:fixed :fixed]
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (check-names writer-edn-schema reader-edn-schema)
  (let [{writer-size :size} writer-edn-schema
        {reader-size :size} reader-edn-schema
        _ (when (not= writer-size reader-size)
            (throw (ex-info
                    (str "Writer and reader fixed schema sizes don't match. "
                         "Writer size: " writer-size ". Reader size: "
                         reader-size ".")
                    (u/sym-map writer-edn-schema reader-edn-schema))))
        deserializer (fn deserialize [is opts]
                       (u/read-bytes is writer-size))]
    (swap! *deserializers assoc [writer-edn-schema reader-edn-schema]
           deserializer)
    deserializer))

(defn deserialize-set [is opts]
  (loop [s (transient #{})]
    (let [long-count (u/read-long-varint-zz is)
          count (int (u/long->int long-count))]
      (if (zero? count)
        (persistent! s)
        (recur (reduce (fn [acc i]
                         (let [k (u/read-utf8-string is)]
                           (conj! acc k)))
                       s (range count)))))))

(defmethod make-deserializer [:map :map]
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (if (and (= :null (:values writer-edn-schema))
           (= :null (:values reader-edn-schema)))
    deserialize-set
    (let [deserialize-value (make-deserializer
                             (:values writer-edn-schema)
                             (:values reader-edn-schema)
                             writer-name->edn-schema
                             reader-name->edn-schema
                             *deserializers)]
      (fn deserialize [is opts]
        (loop [m (transient {})]
          (let [long-count (u/read-long-varint-zz is)
                count (int (u/long->int long-count))]
            (if (zero? count)
              (persistent! m)
              (recur (reduce (fn [acc i]
                               (let [k (u/read-utf8-string is)
                                     v (deserialize-value is opts)]
                                 (assoc! acc k v)))
                             m (range count))))))))))

(defmethod make-deserializer :logical-type
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (let [{:keys [logical-type avro->lt]} reader-edn-schema
        _ (when-not avro->lt
            (throw (ex-info (str "Logical type `" logical-type "` is missing "
                                 "a `avro->lt` attribute.")
                            (u/sym-map logical-type reader-edn-schema))))
        deser (make-deserializer
               writer-edn-schema (u/strip-lt-attrs reader-edn-schema)
               writer-name->edn-schema reader-name->edn-schema *deserializers)
        deserializer (fn deserialize [is opts]
                       (avro->lt (deser is opts)))]
    (swap! *deserializers assoc [writer-edn-schema reader-edn-schema]
           deserializer)
    deserializer))

(defmethod make-deserializer [:array :array]
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (let [deserialize-item (make-deserializer
                          (:items writer-edn-schema)
                          (:items reader-edn-schema)
                          writer-name->edn-schema
                          reader-name->edn-schema
                          *deserializers)]
    (fn deserialize [is opts]
      (loop [a (transient [])]
        (let [count (int (u/long->int (u/read-long-varint-zz is)))]
          (if (zero? count)
            (persistent! a)
            (recur (reduce (fn [acc i]
                             (conj! acc (deserialize-item is opts)))
                           a (range count)))))))))

(defn get-reader-field [reader-fields writer-field-name]
  (let [normalized-writer-field-name (u/fix-field-name writer-field-name)]
    (reduce
     (fn [acc reader-field]
       (let [reader-field-name (:name reader-field)
             normalized-reader-field-name (u/fix-field-name
                                           reader-field-name)]
         (if (= normalized-writer-field-name normalized-reader-field-name)
           (reduced reader-field)
           acc)))
     nil reader-fields)))

(defn make-field-info
  [reader-fields writer-name->edn-schema reader-name->edn-schema *deserializers
   field]
  (let [{writer-field-name :name
         writer-field-type :type} field
        reader-field (get-reader-field reader-fields
                                       writer-field-name)
        [reader-field-type reader-name-map] (if (:type reader-field)
                                              [(:type reader-field)
                                               reader-name->edn-schema]
                                              [writer-field-type
                                               writer-name->edn-schema])
        deserialize (make-deserializer writer-field-type reader-field-type
                                       writer-name->edn-schema reader-name-map
                                       *deserializers)
        reader-k (when reader-field
                   (:name reader-field))]
    (u/sym-map deserialize reader-k)))

(defmethod make-deserializer [:record :record]
  ([writer-edn-schema reader-edn-schema
    writer-name->edn-schema reader-name->edn-schema
    *deserializers]
   (make-deserializer writer-edn-schema reader-edn-schema
                      writer-name->edn-schema reader-name->edn-schema
                      *deserializers false))
  ([writer-edn-schema reader-edn-schema
    writer-name->edn-schema reader-name->edn-schema
    *deserializers in-ambiguous-union?]
   (check-names writer-edn-schema reader-edn-schema)
   (binding [u/**enclosing-namespace** (namespace (:name writer-edn-schema))]
     (let [{writer-fields :fields} writer-edn-schema
           {reader-fields :fields} reader-edn-schema
           ;; Use mapv, not map, below since map doesn't preserve
           ;; the u/**enclosing-namespace** binding due to laziness
           wfis (mapv #(make-field-info reader-fields
                                        writer-name->edn-schema
                                        reader-name->edn-schema
                                        *deserializers
                                        %)
                      writer-fields)
           norm-writer-field-names (->> writer-fields
                                        (map #(-> % :name u/fix-field-name))
                                        (set))
           added (reduce (fn [acc {reader-field-name :name
                                   reader-field-default :default}]
                           (if (or (norm-writer-field-names
                                    (u/fix-field-name reader-field-name))
                                   (nil? reader-field-default))
                             acc
                             (assoc acc reader-field-name reader-field-default)))
                         {}
                         reader-fields)
           deser (fn deserialize [is opts]
                   (let [{:keys [add-record-name]} opts
                         add-name? (or (= :always add-record-name)
                                       (and (= :when-ambiguous add-record-name)
                                            in-ambiguous-union?))]
                     (cond-> (reduce
                              (fn [acc info]
                                (let [{:keys [deserialize reader-k]} info
                                      v (deserialize is opts)]
                                  (if (and reader-k (not (nil? v)))
                                    (assoc! acc reader-k v)
                                    acc)))
                              (transient added)
                              wfis)
                       add-name? (assoc! :deercreeklabs.lancaster/record-name
                                         (:name reader-edn-schema))
                       true (persistent!))))]
       (swap! *deserializers assoc [writer-edn-schema reader-edn-schema] deser)
       deser))))

(defmethod make-deserializer [:other :union]
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (let [info (u/get-union-record-keyset-info
              reader-edn-schema reader-name->edn-schema)
        in-ambiguous-union? (boolean (seq (:overlapping-record-keyset info)))
        last-branch (dec (count reader-edn-schema))]
    (loop [branch 0]
      (let [reader-item-schema (nth reader-edn-schema branch)
            deser (try
                    (let [record? (= :record (u/get-avro-type
                                              reader-item-schema
                                              reader-name->edn-schema))
                          args (cond-> [writer-edn-schema
                                        reader-item-schema
                                        writer-name->edn-schema
                                        reader-name->edn-schema
                                        *deserializers]
                                 record? (conj in-ambiguous-union?))]
                      (apply make-deserializer args))
                    (catch #?(:clj Exception :cljs js/Error) e
                      (if (u/match-exception? e)
                        :no-match
                        (throw e))))
            match? (not= :no-match deser)
            final-branch? (= last-branch branch)]

        (cond
          match?
          deser

          (not final-branch?)
          (recur (inc branch))

          :else
          (fn deserialize [is opts]
            (throw
             (ex-info
              "No schemas in reader union schema match writer."
              (u/sym-map writer-edn-schema reader-edn-schema)))))))))

(defn make-deserializer-union-writer
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (let [mk-des (fn [writer-item-schema]
                 (try
                   (make-deserializer
                    writer-item-schema reader-edn-schema
                    writer-name->edn-schema reader-name->edn-schema
                    *deserializers)
                   (catch #?(:clj Exception :cljs js/Error) e
                     (if-not (u/match-exception? e)
                       (throw e)
                       (fn deserialize [is opts]
                         (throw
                          (ex-info "Reader and writer schemas do not match."
                                   (u/sym-map writer-edn-schema
                                              reader-edn-schema))))))))
        branch->deserializer (mapv mk-des writer-edn-schema)]
    (fn deserialize [is opts]
      (let [branch (u/read-long-varint-zz is)
            deserializer (branch->deserializer branch)]
        (deserializer is opts)))))

(defmethod make-deserializer [:union :other]
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (make-deserializer-union-writer writer-edn-schema
                                  reader-edn-schema
                                  writer-name->edn-schema
                                  reader-name->edn-schema
                                  *deserializers))

(defmethod make-deserializer [:union :union]
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema
   *deserializers]
  (make-deserializer-union-writer writer-edn-schema
                                  reader-edn-schema
                                  writer-name->edn-schema
                                  reader-name->edn-schema
                                  *deserializers))

(defn make-recursive-deserializer
  ([writer-edn-schema reader-edn-schema writer-name->edn-schema
    reader-name->edn-schema *deserializers]
   (make-recursive-deserializer
    writer-edn-schema reader-edn-schema
    writer-name->edn-schema reader-name->edn-schema
    *deserializers false))
  ([writer-edn-schema reader-edn-schema writer-name->edn-schema
    reader-name->edn-schema *deserializers in-ambiguous-union?]
   (if-not (u/edn-schemas-match? writer-edn-schema reader-edn-schema
                                 writer-name->edn-schema reader-name->edn-schema)
     (throw (ex-info "Reader and writer schemas do not match."
                     (u/sym-map writer-edn-schema reader-edn-schema)))
     (fn deserialize [is opts]
       (let [k [writer-edn-schema reader-edn-schema]
             args (cond-> [writer-edn-schema
                           reader-edn-schema
                           writer-name->edn-schema
                           reader-name->edn-schema
                           *deserializers]
                    in-ambiguous-union? (conj in-ambiguous-union?))]
         (if-let [deser (@*deserializers k)]
           (deser is opts)
           (let [deser* (apply make-deserializer args)]
             (swap! *deserializers assoc k deser*)
             (deser* is opts))))))))

(defn kw->edn-schema [kw name->edn-schema]
  (let [fqname (u/qualify-name-kw kw (name->edn-schema kw))]
    (or (name->edn-schema fqname)
        (let [schema-names (keys name->edn-schema)]
          (throw
           (ex-info
            (str "Could not find named schema `" fqname "`.")
            (u/sym-map fqname kw schema-names name->edn-schema)))))))

(defmethod make-deserializer [:name-keyword :other]
  ([writer-name-kw reader-edn-schema writer-name->edn-schema
    reader-name->edn-schema *deserializers]
   (make-deserializer
    writer-name-kw reader-edn-schema
    writer-name->edn-schema reader-name->edn-schema
    *deserializers false))
  ([writer-name-kw reader-edn-schema
    writer-name->edn-schema reader-name->edn-schema
    *deserializers in-ambiguous-union?]
   (let [writer-edn-schema (kw->edn-schema writer-name-kw
                                           writer-name->edn-schema)
         args (cond-> [writer-edn-schema
                       reader-edn-schema
                       writer-name->edn-schema
                       reader-name->edn-schema
                       *deserializers]
                in-ambiguous-union? (conj in-ambiguous-union?))]
     (apply make-recursive-deserializer args))))

(defmethod make-deserializer [:other :name-keyword]
  ([writer-edn-schema reader-name-kw writer-name->edn-schema
    reader-name->edn-schema *deserializers]
   (make-deserializer
    writer-edn-schema reader-name-kw
    writer-name->edn-schema reader-name->edn-schema
    *deserializers false))
  ([writer-edn-schema reader-name-kw
    writer-name->edn-schema reader-name->edn-schema
    *deserializers in-ambiguous-union?]
   (let [reader-edn-schema (kw->edn-schema reader-name-kw
                                           reader-name->edn-schema)
         args (cond-> [writer-edn-schema
                       reader-edn-schema
                       writer-name->edn-schema
                       reader-name->edn-schema
                       *deserializers]
                in-ambiguous-union? (conj in-ambiguous-union?))]
     (apply make-recursive-deserializer args))))

(defmethod make-deserializer [:name-keyword :name-keyword]
  ([writer-name-kw reader-name-kw writer-name->edn-schema
    reader-name->edn-schema *deserializers]
   (make-deserializer writer-name-kw reader-name-kw
                      writer-name->edn-schema reader-name->edn-schema
                      *deserializers false))
  ([writer-name-kw reader-name-kw writer-name->edn-schema
    reader-name->edn-schema *deserializers in-ambiguous-union?]
   (let [writer-edn-schema (kw->edn-schema writer-name-kw
                                           writer-name->edn-schema)
         reader-edn-schema (kw->edn-schema reader-name-kw
                                           reader-name->edn-schema)
         args (cond-> [writer-edn-schema
                       reader-edn-schema
                       writer-name->edn-schema
                       reader-name->edn-schema
                       *deserializers]
                in-ambiguous-union? (conj in-ambiguous-union?))]
     (apply make-recursive-deserializer args))))
