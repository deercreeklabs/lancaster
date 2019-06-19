(ns deercreeklabs.lancaster.deser
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   #?(:cljs [goog.math :as gm])))

#?(:cljs (def Long gm/Long))

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
  (fn deserialize [is]
    nil))

(defmethod make-deserializer [:boolean :boolean]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (= 1 (u/read-byte is))))

(defmethod make-deserializer [:int :int]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (int (u/read-long-varint-zz is))))

(defmethod make-deserializer [:int :long]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (u/read-long-varint-zz is)))

(defmethod make-deserializer [:int :float]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (float (u/read-long-varint-zz is))))

(defmethod make-deserializer [:int :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (double (u/read-long-varint-zz is))))

(defmethod make-deserializer [:long :long]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (u/read-long-varint-zz is)))

(defmethod make-deserializer [:long :float]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (let [l (u/read-long-varint-zz is)]
      #?(:clj (float l)
         :cljs (if (number? l)
                 l
                 (.toNumber ^Long l))))))

(defmethod make-deserializer [:long :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (let [l (u/read-long-varint-zz is)]
      #?(:clj (double l)
         :cljs (if (number? l)
                 l
                 (.toNumber ^Long l))))))

(defmethod make-deserializer [:float :float]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (u/read-float is)))

(defmethod make-deserializer [:float :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (double (u/read-float is))))

(defmethod make-deserializer [:double :double]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (u/read-double is)))

(defmethod make-deserializer [:string :string]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (u/read-utf8-string is)))

(defmethod make-deserializer [:string :bytes]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (ba/utf8->byte-array (u/read-utf8-string is))))

(defmethod make-deserializer [:bytes :bytes]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (u/read-len-prefixed-bytes is)))

(defmethod make-deserializer [:bytes :string]
  [writer-edn-schema reader-edn-schema & _]
  (fn deserialize [is]
    (ba/byte-array->utf8 (u/read-len-prefixed-bytes is))))

(defmethod make-deserializer [:enum :enum]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (check-names writer-edn-schema reader-edn-schema)
  (let [writer-syms (:symbols writer-edn-schema)
        {:keys [symbols default name key-ns-type]} reader-edn-schema
        qualify (partial u/ns-k key-ns-type name)
        q-default (when default
                    (qualify default))
        find-reader-symbol (fn [wsym]
                             (reduce (fn [acc rsym]
                                       (if (= wsym rsym)
                                         (reduced (qualify rsym))
                                         acc))
                                     q-default symbols))
        i->reader-symbol (mapv find-reader-symbol writer-syms)
        deserializer (fn deserialize [is]
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
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (check-names writer-edn-schema reader-edn-schema)
  (let [{writer-size :size
         writer-name :name} writer-edn-schema
        {reader-size :size} reader-edn-schema
        _ (when (not= writer-size reader-size)
            (throw (ex-info
                    (str "Writer and reader fixed schema sizes don't match. "
                         "Writer size: " writer-size ". Reader size: "
                         reader-size ".")
                    (u/sym-map writer-edn-schema reader-edn-schema))))
        deserializer (fn deserialize [is]
                       (u/read-bytes is writer-size))]
    (swap! *deserializers assoc [writer-edn-schema reader-edn-schema]
           deserializer)
    deserializer))

(defmethod make-deserializer [:map :map]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (let [deserialize-value (make-deserializer
                           (:values writer-edn-schema)
                           (:values reader-edn-schema)
                           name->edn-schema *deserializers)]
    (fn deserialize [is]
      (loop [m (transient {})]
        (let [long-count (u/read-long-varint-zz is)
              count (int (u/long->int long-count))]
          (if (zero? count)
            (persistent! m)
            (recur (reduce (fn [acc i]
                             (let [k (u/read-utf8-string is)
                                   v (deserialize-value is)]
                               (assoc! acc k v)))
                           m (range count)))))))))

(defmethod make-deserializer :logical-type
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (let [{:keys [logical-type avro->lt]} reader-edn-schema
        _ (when-not avro->lt
            (throw (ex-info (str "Logical type `" logical-type "` is missing "
                                 "a `avro->lt` attribute.")
                            (u/sym-map logical-type reader-edn-schema))))
        deser (make-deserializer
               writer-edn-schema (u/strip-lt-attrs reader-edn-schema)
               name->edn-schema *deserializers)
        deserializer (fn deserialize [is]
                       (avro->lt (deser is)))]
    (swap! *deserializers assoc [writer-edn-schema reader-edn-schema]
           deserializer)
    deserializer))

(defmethod make-deserializer [:array :array]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (let [deserialize-item (make-deserializer
                          (:items writer-edn-schema)
                          (:items reader-edn-schema)
                          name->edn-schema *deserializers)]
    (fn deserialize [is]
      (loop [a (transient [])]
        (let [count (int (u/long->int (u/read-long-varint-zz is)))]
          (if (zero? count)
            (persistent! a)
            (recur (reduce (fn [acc i]
                             (conj! acc (deserialize-item is)))
                           a (range count)))))))))

(defmethod make-deserializer [:record :record]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (check-names writer-edn-schema reader-edn-schema)
  (binding [u/**enclosing-namespace** (namespace (:name writer-edn-schema))]
    (let [{writer-fields :fields
           writer-name :name} writer-edn-schema
          ens u/**enclosing-namespace**
          {reader-fields :fields
           reader-name :name
           reader-key-ns-type :key-ns-type} reader-edn-schema
          qualify-reader (partial u/ns-k reader-key-ns-type reader-name)
          get-reader-field (fn [writer-field-name]
                             (reduce
                              (fn [acc reader-field]
                                (if (= writer-field-name (:name reader-field))
                                  (reduced reader-field)
                                  acc))
                              nil reader-fields))
          ;; Use mapv, not map, below since map doesn't preserve
          ;; the u/**enclosing-namespace** binding due to laziness
          wfis (mapv (fn [{:keys [name type]}]
                       (let [reader-field (get-reader-field name)
                             reader-field-type (or (:type reader-field)
                                                   type)
                             deserialize (make-deserializer
                                          type reader-field-type
                                          name->edn-schema *deserializers)
                             reader-k (when reader-field
                                        (qualify-reader (:name reader-field)))]
                         (u/sym-map deserialize reader-k)))
                     writer-fields)
          writer-field-names (set (map :name writer-fields))
          added (reduce (fn [acc {:keys [name default]}]
                          (if (writer-field-names name)
                            acc
                            (if default
                              (assoc acc (qualify-reader name) default)
                              (throw (ex-info
                                      (str "Reader record field `" name "` "
                                           "does not exist in writer record "
                                           "and does not have a default value.")
                                      {:writer-edn-schema writer-field-names
                                       :reader-edn-schema reader-edn-schema
                                       :problematic-reader-field-name name})))))
                        {} reader-fields)
          deserializer (fn deserialize [is]
                         (persistent!
                          (reduce (fn [acc info]
                                    (let [{:keys [deserialize reader-k]} info
                                          v (deserialize is)]
                                      (if reader-k
                                        (assoc! acc reader-k v)
                                        acc)))
                                  (transient added) wfis)))]
      (swap! *deserializers assoc [writer-edn-schema reader-edn-schema]
             deserializer)
      deserializer)))

(defn branch-meta [branch-index edn-schema]
  (when-not (:avro->lt edn-schema)
    (case (u/get-avro-type edn-schema)
      :map {:short-name :map :fq-name :map :branch-index branch-index}
      :record (let [fq-name (:name edn-schema)
                    short-name (keyword (name fq-name))]
                (u/sym-map short-name fq-name branch-index))
      nil)))

(defmethod make-deserializer [:other :union]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (loop [branch 0]
    (let [reader-item-schema (reader-edn-schema branch)
          deser (try
                  (make-deserializer writer-edn-schema
                                     reader-item-schema
                                     name->edn-schema *deserializers)
                  (catch #?(:clj Exception :cljs js/Error) e
                    (when-not (u/match-exception? e)
                      (throw e))))]
      (if deser
        (if-let [metadata (branch-meta branch reader-item-schema)]
          (fn deserialize [is]
            (with-meta (deser is) metadata))
          deser)
        (if (< (inc branch) (count reader-edn-schema))
          (recur (inc branch))
          (fn deserialize [is]
            (throw
             (ex-info "No schemas in reader union schema match writer."
                      (u/sym-map writer-edn-schema reader-edn-schema)))))))))

(defn make-deserializer-union-writer
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (let [mk-des (fn [writer-item-schema]
                 (try
                   (make-deserializer
                    writer-item-schema reader-edn-schema name->edn-schema
                    *deserializers)
                   (catch #?(:clj Exception :cljs js/Error) e
                     (if-not (u/match-exception? e)
                       (throw e)
                       (fn deserialize [is]
                         (throw
                          (ex-info "Reader and writer schemas do not match."
                                   (u/sym-map writer-edn-schema
                                              reader-edn-schema))))))))
        branch->deserializer (mapv mk-des writer-edn-schema)]
    (fn deserialize [is]
      (let [branch (u/read-long-varint-zz is)
            deserializer (branch->deserializer branch)]
        (deserializer is)))))

(defmethod make-deserializer [:union :other]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (make-deserializer-union-writer writer-edn-schema reader-edn-schema
                                  name->edn-schema *deserializers))

(defmethod make-deserializer [:union :union]
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (make-deserializer-union-writer writer-edn-schema reader-edn-schema
                                  name->edn-schema *deserializers))

(defn make-recursive-deserializer
  [writer-edn-schema reader-edn-schema name->edn-schema *deserializers]
  (if-not (u/edn-schemas-match? writer-edn-schema reader-edn-schema
                                name->edn-schema)
    (throw (ex-info "Reader and writer schemas do not match."
                    (u/sym-map writer-edn-schema reader-edn-schema)))
    (fn deserialize [is]
      (let [k [writer-edn-schema reader-edn-schema]]
        (if-let [deser (@*deserializers k)]
          (deser is)
          (let [deser* (make-deserializer writer-edn-schema reader-edn-schema
                                          name->edn-schema *deserializers)]
            (swap! *deserializers assoc k deser*)
            (deser* is)))))))

(defmethod make-deserializer [:name-keyword :other]
  [writer-name-kw reader-edn-schema name->edn-schema *deserializers]
  (let [writer-edn-schema (-> (u/qualify-name-kw writer-name-kw)
                              (name->edn-schema))]
    (make-recursive-deserializer writer-edn-schema reader-edn-schema
                                 name->edn-schema *deserializers)))

(defmethod make-deserializer [:other :name-keyword]
  [writer-edn-schema reader-name-kw name->edn-schema *deserializers]
  (let [reader-edn-schema (-> (u/qualify-name-kw reader-name-kw)
                              (name->edn-schema))]
    (make-recursive-deserializer writer-edn-schema reader-edn-schema
                                 name->edn-schema *deserializers)))

(defmethod make-deserializer [:name-keyword :name-keyword]
  [writer-name-kw reader-name-kw name->edn-schema *deserializers]
  (let [get-edn #(-> (u/qualify-name-kw %)
                     (name->edn-schema))
        writer-edn-schema (get-edn writer-name-kw)
        reader-edn-schema (get-edn reader-name-kw)]
    (make-recursive-deserializer writer-edn-schema reader-edn-schema
                                 name->edn-schema *deserializers)))
