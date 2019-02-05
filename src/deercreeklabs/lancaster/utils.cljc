(ns deercreeklabs.lancaster.utils
  (:refer-clojure :exclude [long])
  (:require
   [camel-snake-kebab.core :as csk]
   #?(:clj [cheshire.core :as json])
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   #?(:cljs [goog.math :as gm])
   #?(:clj [primitive-math :as pm])
   [schema.core :as s])
  #?(:cljs
     (:require-macros
      [deercreeklabs.lancaster.utils :refer [sym-map]])))

#?(:cljs
   (set! *warn-on-infer* true))

#?(:cljs (def class type))
#?(:cljs (def Long gm/Long))

#?(:cljs (def max-int (gm/Long.fromInt 2147483647)))
#?(:cljs (def min-int (gm/Long.fromInt -2147483648)))

#?(:clj (pm/use-primitive-operators))

(declare default-data)

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defprotocol ILancasterSchema
  (serialize [this data] [this os data])
  (deserialize [this writer-pcf is])
  (wrap [this data])
  (edn-schema [this])
  (json-schema [this])
  (parsing-canonical-form [this])
  (fingerprint64 [this])
  (plumatic-schema [this]))

(defprotocol IOutputStream
  (write-byte [this b])
  (write-bytes [this bs num-bytes])
  (write-bytes-w-len-prefix [this bs])
  (write-utf8-string [this s])
  (write-float [this f])
  (write-double [this d])
  (to-byte-array [this]))

(defprotocol IInputStream
  (mark [this])
  (read-byte [this])
  (read-bytes [this num-bytes])
  (read-len-prefixed-bytes [this])
  (read-utf8-string [this])
  (read-float [this])
  (read-double [this])
  (reset-to-mark! [this]))

(def ^:dynamic **enclosing-namespace** nil)
(def avro-container-types #{:record :array :map :union :flex-map})
(def avro-named-types #{:record :fixed :enum :flex-map})
(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-primitive-type-strings (into #{} (map name avro-primitive-types)))

(def Nil (s/eq nil))

(s/defn ex-msg :- s/Str
  [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(s/defn ex-stacktrace :- s/Str
  [e]
  #?(:clj (clojure.string/join "\n" (map str (.getStackTrace ^Exception e)))
     :cljs (.-stack e)))

(s/defn ex-msg-and-stacktrace :- s/Str
  [e]
  (str "\nException:\n"
       (ex-msg e)
       "\nStacktrace:\n"
       (ex-stacktrace e)))

(s/defn current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn schema-name [clj-name]
  (-> (name clj-name)
      (clojure.string/split #"-schema")
      (first)))

(defn fullname? [s]
  (clojure.string/includes? s "."))

(defn fullname->ns [fullname]
  (if-not (fullname? fullname)
    nil ;; no namespace
    (let [parts (clojure.string/split fullname #"\.")]
      (clojure.string/join "." (butlast parts)))))

(defn fullname->name [fullname]
  (if-not (fullname? fullname)
    nil ;; no namespace
    (let [parts (clojure.string/split fullname #"\.")]
      (last parts))))

(defn qualify-name-kw [name-kw]
  (cond
    (qualified-keyword? name-kw)
    name-kw

    (simple-keyword? name-kw)
    (if **enclosing-namespace**
      (keyword (name **enclosing-namespace**) (name name-kw))
      name-kw)

    :else
    (throw (ex-info (str "Argument to qualify-name-kw (" name-kw
                         ") is not a keyword")
                    (sym-map name-kw)))))

(defn name-keyword? [x]
  (and (keyword? x)
       (not (avro-primitive-types x))))

(defn clj-namespace->java-namespace [ns]
  (when ns
    (clojure.string/replace (str ns) #"-" "_")))

(defn java-namespace->clj-namespace [ns]
  (when ns
    (clojure.string/replace (str ns) #"_" "-")))

(defn edn-name-str->avro-name [s]
  (if (fullname? s)
    (let [name-parts (clojure.string/split s #"\.")
          schema-ns (clojure.string/join "." (butlast name-parts))
          schema-name (last name-parts)]
      (str (clj-namespace->java-namespace schema-ns) "."
           (csk/->PascalCase schema-name)))
    (csk/->PascalCase s)))

(defn edn-name-kw->avro-name [kw]
  (let [kw-ns (clj-namespace->java-namespace (namespace kw))
        kw-name (csk/->PascalCase (name kw))]
    (if kw-ns
      (str kw-ns "." kw-name)
      kw-name)))

(s/defn get-avro-type :- s/Keyword
  [edn-schema]
  (cond
    (sequential? edn-schema) :union
    (map? edn-schema) (:type edn-schema)
    (nil? edn-schema) (throw
                       (ex-info "Schema argument to get-avro-type is nil."
                                {:type :illegal-schema
                                 :subtype :schema-is-nil
                                 :schema edn-schema}))
    (string? edn-schema) :name-string ;; For Avro schemas
    (avro-primitive-types edn-schema) edn-schema
    (keyword? edn-schema) :name-keyword
    :else (throw (ex-info (str "Failed to get avro type for schema: "
                               edn-schema)
                          (sym-map edn-schema)))))

(s/defn edn-schema->name-kw :- s/Keyword
  [edn-schema]
  (cond
    (avro-named-types (:type edn-schema))
    (if-let [schema-ns (:namespace edn-schema)]
      (keyword (name schema-ns) (name (:name edn-schema)))
      (:name edn-schema))

    (map? edn-schema)
    (:type edn-schema)

    (sequential? edn-schema)
    :union

    (nil? edn-schema)
    (throw (ex-info "Schema is nil."
                    {:type :illegal-argument
                     :subtype :schema-is-nil
                     :schema edn-schema}))

    :else
    edn-schema))

(s/defn name-kw->name-str :- s/Str
  [kw :- s/Keyword]
  (cond
    (simple-keyword? kw) (name kw)
    (qualified-keyword? kw) (str (namespace kw) "." (name kw))
    :else (throw (ex-info (str "Argument (" kw ") is not a keyword.")
                          {:arg kw}))))

(defn byte-array->byte-str [ba]
  (apply str (map char ba)))

(defn ensure-edn-schema [schema]
  (cond
    (satisfies? ILancasterSchema schema)
    (edn-schema schema)

    (sequential? schema)
    (mapv ensure-edn-schema schema) ;; union

    :else
    schema))

(defn make-default-fixed-or-bytes [num-bytes default]
  (byte-array->byte-str (or default
                            (ba/byte-array (take num-bytes (repeat 0))))))

(defn make-default-record [record-edn-schema default-record]
  (reduce (fn [acc field]
            (let [{field-name :name
                   field-type :type
                   field-default :default} field
                  field-schema (ensure-edn-schema field-type)
                  v (if default-record
                      (default-data field-schema
                                    (field-name default-record))
                      field-default)]
              (assoc acc field-name v)))
          {} (:fields record-edn-schema)))

(defn default-data
  ([edn-schema]
   (default-data edn-schema nil {}))
  ([edn-schema field-default]
   (default-data edn-schema field-default {}))
  ([edn-schema field-default name->edn-schema]
   (let [avro-type (get-avro-type edn-schema)]
     (case avro-type
       :record (make-default-record edn-schema field-default)
       :union (default-data (first edn-schema) field-default)
       :fixed (make-default-fixed-or-bytes (:size edn-schema) field-default)
       :bytes (make-default-fixed-or-bytes 0 field-default)
       (or field-default
           (case avro-type
             :null nil
             :boolean false
             :int (int -1)
             :long -1
             :float (float -1.0)
             :double (double -1.0)
             :string ""
             :enum (first (:symbols edn-schema))
             :array []
             :map {}
             :flex-map {}
             :name-keyword (default-data (name->edn-schema edn-schema)
                                         field-default
                                         name->edn-schema)))))))

(defn first-arg-dispatch [first-arg & rest-of-args]
  first-arg)

(defn avro-type-dispatch [edn-schema & args]
  (get-avro-type edn-schema))

(defmulti make-serializer avro-type-dispatch)
(defmulti make-deserializer avro-type-dispatch)
(defmulti edn-schema->avro-schema avro-type-dispatch)
(defmulti edn-schema->plumatic-schema avro-type-dispatch)
(defmulti make-default-data-size avro-type-dispatch)
(defmulti make-edn-schema first-arg-dispatch)

(s/defn long? :- s/Bool
  [x :- s/Any]
  (if x
    (boolean (= Long (class x)))
    false))

(s/defn long-or-int? :- s/Bool
  "Is the argument a long or an integer?"
  [x :- s/Any]
  (or (long? x)
      (integer? x)))

(def LongOrInt (s/pred long-or-int?))

(defn valid-int? [data]
  (and (integer? data)
       (<= (int data) (int 2147483647))
       (>= (int data) (int -2147483648))))

(defn valid-long? [data]
  (long-or-int? data))

(defn valid-float? [data]
  (and (number? data)
       (<= (float data) (float 3.4028234E38))
       (>= (float data) (float -3.4028234E38))))

(defn valid-double? [data]
  (number? data))

(defn valid-bytes-or-string? [data]
  (or (string? data)
      (ba/byte-array? data)))

(def StringOrBytes (s/pred valid-bytes-or-string?))

(defn valid-array? [data]
  (sequential? data))

(defn valid-map? [data]
  (and (map? data)
       (if (pos? (count data))
         (string? (-> data first first))
         true)))

(defn valid-record? [data]
  (and (map? data)
       (if (pos? (count data))
         (keyword? (-> data first first))
         true)))

(s/defn long= :- s/Bool
  [a :- s/Any
   b :- s/Any]
  #?(:clj (= a b)
     :cljs (cond
             (long? a) (.equals ^Long a b)
             (long? b) (.equals ^Long b a)
             :else (= a b))))

#?(:cljs (extend-type Long
           IEquiv
           (-equiv [l other]
             (long= l other))))

#?(:cljs (extend-type Long
           IHash
           (-hash [l]
             (bit-xor (.getLowBits l) (.getHighBits l)))))

#?(:cljs (extend-type Long
           IComparable
           (-compare [l other]
             (.compare l other))))

(s/defn ints->long :- Long
  [high :- s/Int
   low :- s/Int]
  #?(:clj (bit-or (bit-shift-left (long high) 32)
                  (bit-and low 0xFFFFFFFF))
     :cljs (.fromBits Long low high)))

(s/defn long->ints :- (s/pair s/Int :high-int
                              s/Int :low-int)
  [l :- Long]
  (let [high (int #?(:clj (bit-shift-right l 32)
                     :cljs (.getHighBits l)))
        low (int #?(:clj (.intValue l)
                    :cljs (.getLowBits l)))]
    [high low]))

(s/defn str->long :- Long
  [s :- s/Str]
  #?(:clj (Long/parseLong s)
     :cljs (.fromString Long s)))

(s/defn long->str :- s/Str
  [l :- Long]
  (.toString l))

(defn- throw-long->int-err [l]
  (throw (ex-info (str "Cannot convert long `" l "` to int.")
                  {:input l
                   :class-of-input (class l)})))

(s/defn long->int :- s/Int
  [l :- LongOrInt]
  (if-not (long? l)
    l
    #?(:clj (if (and (<= ^Long l 2147483647) (>= ^Long l -2147483648))
              (.intValue ^Long l)
              (throw-long->int-err l))
       :cljs (if (and (.lessThanOrEqual ^Long l 2147483647)
                      (.greaterThanOrEqual ^Long l -2147483648))
               (.toInt ^Long l)
               (throw-long->int-err ^Long l)))))

(defn int->long [int]
  #?(:clj (clojure.core/long int)
     :cljs (.fromInt Long int)))

(defn more-than-one? [schema-set edn-schemas]
  (> (count (keep #(schema-set (get-avro-type %)) edn-schemas))
     1))

(defn contains-union? [edn-schemas]
  (some #(= :union (get-avro-type %)) edn-schemas))

(defn wrapping-required? [edn-schemas]
  (or (more-than-one? #{:map :record} edn-schemas)
      (more-than-one? #{:int :long :float :double} edn-schemas)
      (more-than-one? #{:bytes :fixed :string} edn-schemas)))

#?(:cljs
   (defn write-long-varint-zz-long [output-stream ^Long l]
     (let [zz-n (.xor (.shiftLeft l 1)
                      (.shiftRight l 63))]
       (loop [^Long n zz-n]
         (if (.isZero (.and n (Long.fromInt -128)))
           (let [b (.and n (Long.fromInt 127))]
             (write-byte output-stream b))
           (let [b (-> (.and n (Long.fromInt 127))
                       (.or (Long.fromInt 128)))]
             (write-byte output-stream b)
             (recur (.shiftRightUnsigned n 7))))))))

(defn write-long-varint-zz* [output-stream l]
  (let [zz-n (bit-xor (bit-shift-left l 1) (bit-shift-right l 63))]
    (loop [n zz-n]
      (if (zero? (bit-and n -128))
        (let [b (bit-and n 127)]
          (write-byte output-stream b))
        (let [b (-> (bit-and n 127)
                    (bit-or 128))]
          (write-byte output-stream b)
          (recur (unsigned-bit-shift-right n 7)))))))

(defn write-long-varint-zz [output-stream l]
  #?(:clj
     (let [l (if (instance? BigInteger l)
               (.longValue ^BigInteger l)
               l)]
       (write-long-varint-zz* output-stream l))
     :cljs
     (do
       (let [l (if-not (long? l)
                 l
                 (if (and (.lessThanOrEqual ^Long l max-int)
                          (.greaterThanOrEqual ^Long l min-int))
                   (.toInt ^Long l)
                   l))]
         (if (long? l)
           (write-long-varint-zz-long output-stream l)
           (write-long-varint-zz* output-stream l))))))

#?(:cljs
   (defn read-long-varint-zz-long [input-stream]
     (loop [i 0
            ^Long out (.getZero Long)]
       (let [^Long b (.fromNumber Long (read-byte input-stream))]
         (if (.isZero (.and b (.fromInt Long 128)))
           (let [zz-n (-> (.shiftLeft b i)
                          (.or out))
                 long-out (->> (.and zz-n (.getOne Long))
                               (.subtract (.getZero Long))
                               (.xor (.shiftRightUnsigned zz-n 1)))]
             long-out)
           (let [out (-> (.and b (.fromInt Long 127))
                         (.shiftLeft i)
                         (.or out))
                 i (+ i 7)]
             (if (<= i 63)
               (recur i out)
               (throw (ex-info "Variable-length quantity is more than 64 bits"
                               (sym-map i))))))))))

(defn read-long-varint-zz [input-stream]
  (mark input-stream)
  (loop [i 0
         out 0]
    (let [b (read-byte input-stream)]
      (if (zero? (bit-and b 128))
        (let [zz-n (-> (bit-shift-left b i)
                       (bit-or out))
              long-out (->> (bit-and zz-n 1)
                            (- 0)
                            (bit-xor (unsigned-bit-shift-right zz-n 1)))]
          long-out)
        (let [out (-> (bit-and b 127)
                      (bit-shift-left i)
                      (bit-or out))
              i (+ 7 i)]
          #?(:cljs
             (if (<= i 31)
               (recur i out)
               (do
                 (reset-to-mark! input-stream)
                 (read-long-varint-zz-long input-stream)))
             :clj
             (if (<= i 63)
               (recur i out)
               (throw (ex-info "Variable-length quantity is more than 64 bits"
                               (sym-map i))))))))))

(defn throw-invalid-data-error [edn-schema data path]
  (let [expected (get-avro-type edn-schema)
        display-data (if (nil? data)
                       "nil"
                       data)
        type #?(:clj (if (nil? data)
                       "nil"
                       (.toString ^Class (class data)))
                :cljs (goog/typeOf data))]
    (throw
     (ex-info (str "Data `" display-data "` (type: " type ") is not a valid "
                   expected ". Path: " path)
              (sym-map expected data type path edn-schema)))))

(defn edn->json-string [edn]
  #?(:clj (json/generate-string edn)
     :cljs (js/JSON.stringify (clj->js edn))))

(defn json-schema->avro-schema [json-str]
  #?(:clj (json/parse-string json-str true)
     :cljs (js->clj (js/JSON.parse json-str) :keywordize-keys true)))

(defmethod make-serializer :null
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (nil? data)
      (throw-invalid-data-error edn-schema data path))))

(defmethod make-deserializer :null
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    nil))

(defmethod make-serializer :boolean
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (boolean? data)
      (throw
       (throw-invalid-data-error edn-schema data path)))
    (write-byte os (if data 1 0))))

(defmethod make-deserializer :boolean
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (= 1 (read-byte is))))

(defmethod make-serializer :int
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-int? data)
      (throw-invalid-data-error edn-schema data path))
    (write-long-varint-zz os data)))

(defmethod make-deserializer :int
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (int (read-long-varint-zz is))))

(defmethod make-serializer :long
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-long? data)
      (throw-invalid-data-error edn-schema data path))
    (write-long-varint-zz os data)))

(defmethod make-deserializer :long
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (read-long-varint-zz is)))

(defmethod make-serializer :float
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-float? data)
      (throw-invalid-data-error edn-schema data path))
    (write-float os data)))

(defmethod make-deserializer :float
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (read-float is)))

(defmethod make-serializer :double
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-double? data)
      (throw-invalid-data-error edn-schema data path))
    (write-double os data)))

(defmethod make-deserializer :double
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (read-double is)))

(defmethod make-serializer :string
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (string? data)
      (throw-invalid-data-error edn-schema data path))
    (write-utf8-string os data)))

(defmethod make-deserializer :string
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (read-utf8-string is)))

(defmethod make-serializer :bytes
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (ba/byte-array? data)
      (throw-invalid-data-error edn-schema data path))
    (write-bytes-w-len-prefix os data)))

(defmethod make-deserializer :bytes
  [edn-schema *name->deserializer]
  (fn deserialize [is]
    (read-len-prefixed-bytes is)))

(defmethod make-serializer :enum
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [symbols name]} edn-schema
        symbol->index (apply hash-map
                             (apply concat (map-indexed
                                            #(vector %2 %1) symbols)))
        serializer (fn serialize [os data path]
                     (if-let [i (symbol->index data)]
                       (write-long-varint-zz os i)
                       (throw
                        (let [display-data (if (nil? data)
                                             "nil"
                                             data)]
                          (ex-info
                           (str "Data (" display-data ") is not one of the "
                                "symbols of this enum. Path: " path)
                           (sym-map data path symbols edn-schema))))))]
    (swap! *name->serializer assoc name serializer)
    serializer))

(defmethod make-deserializer :enum
  [edn-schema *name->deserializer]
  (let [{:keys [symbols name]} edn-schema
        index->symbol (apply hash-map
                             (apply concat (map-indexed vector symbols)))
        deserializer (fn deserialize [is]
                       (index->symbol (read-long-varint-zz is)))]
    (swap! *name->deserializer assoc name deserializer)
    deserializer))

(defmethod make-serializer :fixed
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [size name]} edn-schema
        serializer (fn serialize [os data path]
                     (when-not (ba/byte-array? data)
                       (throw-invalid-data-error edn-schema data path))
                     (when-not (= size (count data))
                       (throw
                        (ex-info
                         (str "Data (" (ba/byte-array->debug-str data)
                              ") is not the proper size (" size ". Path: " path)
                         {:data data
                          :data-size (count data)
                          :schema-size size
                          :path path})))
                     (write-bytes os data size))]
    (swap! *name->serializer assoc name serializer)
    serializer))

(defmethod make-deserializer :fixed
  [edn-schema *name->deserializer]
  (let [{:keys [size name]} edn-schema
        deserializer (fn deserialize [is]
                       (read-bytes is size))]
    (swap! *name->deserializer assoc name deserializer)
    deserializer))

(defmethod make-serializer :map
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [values]} edn-schema
        serialize-value (make-serializer values name->edn-schema
                                         *name->serializer)]
    (fn serialize [os data path]
      (when-not (valid-map? data)
        (throw-invalid-data-error edn-schema data path))
      (when (pos? (count data))
        (write-long-varint-zz os (count data))
        (doseq [[k v] data]
          (write-utf8-string os k)
          (serialize-value os v (conj path k))))
      (write-byte os 0))))

(defmethod make-deserializer :map
  [edn-schema *name->deserializer]
  (let [{:keys [values]} edn-schema
        deserialize-value (make-deserializer values *name->deserializer)]
    (fn deserialize [is]
      (loop [m (transient {})]
        (let [long-count (read-long-varint-zz is)
              count (int (long->int long-count))]
          (if (zero? count)
            (persistent! m)
            (recur (reduce (fn [acc i]
                             (let [k (read-utf8-string is)
                                   v (deserialize-value is)]
                               (assoc! acc k v)))
                           m (range count)))))))))

(defn flex-map-edn-schema->record-edn-schema [fm-edn-schema]
  (let [keys-schema (:keys fm-edn-schema)
        values-schema (:values fm-edn-schema)
        fields [[:ks (make-edn-schema :array nil keys-schema)]
                [:vs (make-edn-schema :array nil values-schema)]]]
    (make-edn-schema :record (:name fm-edn-schema) fields)))

(defmethod make-serializer :flex-map
  [edn-schema name->edn-schema *name->serializer]
  (let [ser-rec (-> (flex-map-edn-schema->record-edn-schema edn-schema)
                    (make-serializer name->edn-schema *name->serializer))]
    (fn serialize [os data path]
      (when-not (map? data)
        (throw-invalid-data-error edn-schema data path))
      (let [ks (keys data)
            vs (vals data)]
        (ser-rec os (sym-map ks vs) path)))))

(defmethod make-deserializer :flex-map
  [edn-schema *name->deserializer]
  (let [deser-rec (-> (flex-map-edn-schema->record-edn-schema edn-schema)
                      (make-deserializer *name->deserializer))]
    (fn deserialize [is]
      (let [{:keys [ks vs]} (deser-rec is)]
        (zipmap ks vs)))))

(defmethod make-serializer :array
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [items]} edn-schema
        serialize-item (make-serializer items name->edn-schema
                                        *name->serializer)]
    (fn serialize [os data path]
      (when-not (sequential? data)
        (throw-invalid-data-error edn-schema data path))
      (when (pos? (count data))
        (write-long-varint-zz os (count data))
        (doall
         (map-indexed (fn [i item]
                        (serialize-item os item (conj path i)))
                      data)))
      (write-byte os 0))))

(defmethod make-deserializer :array
  [edn-schema *name->deserializer]
  (let [{:keys [items]} edn-schema
        deserialize-item (make-deserializer items *name->deserializer)]
    (fn deserialize [is]
      (loop [a (transient [])]
        (let [count (int (long->int (read-long-varint-zz is)))]
          (if (zero? count)
            (persistent! a)
            (recur (reduce (fn [acc i]
                             (conj! acc (deserialize-item is)))
                           a (range count)))))))))
(def avro-type->pred
  {:null nil?
   :boolean boolean?
   :int int?
   :long long-or-int?
   :float number?
   :double number?
   :bytes valid-bytes-or-string?
   :string valid-bytes-or-string?
   :enum keyword?
   :fixed valid-bytes-or-string?
   :array sequential?
   :map map?
   :record map?
   :flex-map map?
   nil (constantly true)})

(defn edn-schema->pred [edn-schema name->edn-schema]
  (let [avro-type (get-avro-type edn-schema)]
    (avro-type->pred (if (not= :name-keyword avro-type)
                       avro-type
                       (when-let [edn-schema (name->edn-schema edn-schema)]
                         (get-avro-type edn-schema))))))

(defn make-data->branch [member-schemas name->edn-schema]
  (let [max-index (dec (count member-schemas))
        tests (mapv #(edn-schema->pred % name->edn-schema) member-schemas)]
    (fn [data path]
      (loop [i 0]
        (cond
          ((tests i) data) i
          (< i max-index) (recur (inc i))
          :else (throw
                 (ex-info (str "Data (" data ") does not match union schema."
                               " Path: " path)
                          (sym-map data path member-schemas))))))))

(defn make-schema-name->branch-info [edn-schema name->edn-schema
                                     *name->serializer]
  (reduce (fn [acc i]
            (let [sch (nth edn-schema i)
                  serializer (make-serializer sch name->edn-schema
                                              *name->serializer)]
              (assoc acc (edn-schema->name-kw sch) [i serializer])))
          {} (range (count edn-schema))))

(defmethod make-serializer :union
  [edn-schema name->edn-schema *name->serializer]
  (if (wrapping-required? edn-schema)
    (let [schema-name->branch-info (make-schema-name->branch-info
                                    edn-schema name->edn-schema
                                    *name->serializer)]
      (fn serialize [os data path]
        (try
          (let [[schema-name data] data
                [branch serializer] (schema-name->branch-info schema-name)]
            (when-not branch
              (let [schema-names (keys schema-name->branch-info)]
                (throw (ex-info (str "Schema name `" schema-name "` is not in "
                                     "the union schema.")
                                (sym-map schema-name schema-names data path
                                         edn-schema)))))
            (write-long-varint-zz os branch)
            (serializer os data path))
          (catch #?(:clj UnsupportedOperationException :cljs js/Error) e
            (throw
             (if-not (str/includes? (ex-msg e) "nth not supported")
               e
               (ex-info "Union requires wrapping, but data is not wrapped."
                        (sym-map data path edn-schema))))))))
    (let [data->branch (make-data->branch edn-schema name->edn-schema)
          branch->serializer (mapv #(make-serializer % name->edn-schema
                                                     *name->serializer)
                                   edn-schema)]
      (fn serialize [os data path]
        (let [branch (data->branch data path)
              serializer (branch->serializer branch)]
          (write-long-varint-zz os branch)
          (serializer os data path))))))

(defmethod make-deserializer :union
  [edn-schema *name->deserializer]
  (let [branch->deserializer (mapv #(make-deserializer % *name->deserializer)
                                   edn-schema)
        branch->schema-name (mapv edn-schema->name-kw edn-schema)]
    (fn deserialize [is]
      (let [branch (read-long-varint-zz is)
            deserializer (branch->deserializer branch)
            data (deserializer is)]
        (if-not (wrapping-required? edn-schema)
          data
          (let [schema-name (branch->schema-name branch)]
            [schema-name data]))))))

(defn make-field-info [field-schema name->edn-schema *name->serializer]
  (let [{:keys [name type default]} field-schema
        serializer (make-serializer type name->edn-schema *name->serializer)]
    [name default serializer]))

(defmethod make-serializer :record
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [fields name]} edn-schema
        field-infos (binding [**enclosing-namespace** (namespace name)]
                      (mapv #(make-field-info % name->edn-schema
                                              *name->serializer)
                            fields))
        serializer (fn serialize [os data path]
                     (when-not (map? data)
                       (throw-invalid-data-error edn-schema data path))
                     (doseq [[k default serializer] field-infos]
                       (serializer os (get data k) (conj path k))))]
    (swap! *name->serializer assoc name serializer)
    serializer))

(defmethod make-deserializer :record
  [edn-schema *name->deserializer]
  (let [{:keys [fields name]} edn-schema
        field-infos (binding [**enclosing-namespace** (namespace name)]
                      (mapv (fn [field]
                              (let [{:keys [name type]} field
                                    deserializer (make-deserializer
                                                  type *name->deserializer)]
                                [name deserializer]))
                            fields))
        deserializer (fn deserialize [is]
                       (persistent!
                        (reduce (fn [acc [k deserializer]]
                                  (assoc! acc k (deserializer is)))
                                (transient {}) field-infos)))]
    (swap! *name->deserializer assoc name deserializer)
    deserializer))

(defmethod make-serializer :name-keyword
  [name-kw name->edn-schema *name->serializer]
  (let [qualified-name-kw (qualify-name-kw name-kw)]
    (fn serialize [os data path]
      (let [serializer (@*name->serializer qualified-name-kw)]
        (serializer os data path)))))

(defmethod make-deserializer :name-keyword
  [name-kw *name->deserializer]
  (let [qualified-name-kw (qualify-name-kw name-kw)]
    (fn deserialize [is]
      (let [deserializer (@*name->deserializer qualified-name-kw)]
        (deserializer is)))))

(defmethod edn-schema->plumatic-schema :null
  [edn-schema name->edn-schema]
  Nil)

(defmethod edn-schema->plumatic-schema :boolean
  [edn-schema name->edn-schema]
  s/Bool)

(defmethod edn-schema->plumatic-schema :int
  [edn-schema name->edn-schema]
  s/Int)

(defmethod edn-schema->plumatic-schema :long
  [edn-schema name->edn-schema]
  LongOrInt)

(defmethod edn-schema->plumatic-schema :float
  [edn-schema name->edn-schema]
  s/Num)

(defmethod edn-schema->plumatic-schema :double
  [edn-schema name->edn-schema]
  s/Num)

(defmethod edn-schema->plumatic-schema :bytes
  [edn-schema name->edn-schema]
  StringOrBytes)

(defmethod edn-schema->plumatic-schema :string
  [edn-schema name->edn-schema]
  StringOrBytes)

(defmethod edn-schema->plumatic-schema :enum
  [edn-schema name->edn-schema]
  (apply s/enum (:symbols edn-schema)))

(defmethod edn-schema->plumatic-schema :fixed
  [edn-schema name->edn-schema]
  StringOrBytes)

(defmethod edn-schema->plumatic-schema :array
  [edn-schema name->edn-schema]
  [(edn-schema->plumatic-schema (:items edn-schema) name->edn-schema)])

(defmethod edn-schema->plumatic-schema :map
  [edn-schema name->edn-schema]
  {s/Str (edn-schema->plumatic-schema (:values edn-schema) name->edn-schema)})

(defmethod edn-schema->plumatic-schema :flex-map
  [edn-schema name->edn-schema]
  (let [{:keys [keys values]} edn-schema
        k-pschema (edn-schema->plumatic-schema keys name->edn-schema)
        v-pschema (edn-schema->plumatic-schema values name->edn-schema)]
    {k-pschema v-pschema}))

(defmethod edn-schema->plumatic-schema :record
  [edn-schema name->edn-schema]
  (reduce (fn [acc {:keys [name type]}]
            (let [key-fn (if (and (= :union (get-avro-type type))
                                  (= :null (first type)))
                           s/optional-key
                           s/required-key)]
              (assoc acc (key-fn name)
                     (edn-schema->plumatic-schema type name->edn-schema))))
          {s/Any s/Any} (:fields edn-schema)))

(defmethod edn-schema->plumatic-schema :name-keyword
  [name-kw name->edn-schema]
  ;; Schemas are looser than optimal, due to recursion issues
  (if-let [edn-schema (name->edn-schema name-kw)]
    (case (get-avro-type edn-schema)
      :record {s/Any s/Any}
      :fixed StringOrBytes
      :enum s/Keyword)
    s/Any))

(defn make-wrapped-union-pred [edn-schema]
  (let [schema-name (edn-schema->name-kw edn-schema)]
    (fn [[data-name data]]
      (= schema-name data-name))))

(defn edn-schema->pred-and-plumatic-schema [edn-schema wrap? name->edn-schema]
  (let [pred (if wrap?
               (make-wrapped-union-pred edn-schema)
               (edn-schema->pred edn-schema name->edn-schema))
        pschema (edn-schema->plumatic-schema edn-schema name->edn-schema)
        pschema (if wrap?
                  [(s/one s/Keyword :schema-name) (s/one pschema :schema)]
                  pschema)]
    [pred pschema]))

(defmethod edn-schema->plumatic-schema :union
  [edn-schema name->edn-schema]
  (apply s/conditional (mapcat
                        #(edn-schema->pred-and-plumatic-schema
                          % (wrapping-required? edn-schema) name->edn-schema)
                        edn-schema)))

(defn get-schemas! [edn-schema *name->edn-schema]
  (let [avro-type (get-avro-type edn-schema)]
    (when (avro-named-types avro-type)
      (swap! *name->edn-schema assoc (:name edn-schema) edn-schema))
    (let [child-schemas (case avro-type
                          :record (map :type (:fields edn-schema))
                          :array [(:items edn-schema)]
                          :map [(:values edn-schema)]
                          :union edn-schema
                          [])]
      (doseq [child-schema child-schemas]
        (get-schemas! child-schema *name->edn-schema)))))

(defn make-name->edn-schema [edn-schema]
  (let [*name->edn-schema (atom {})]
    (get-schemas! edn-schema *name->edn-schema)
    @*name->edn-schema))

(defn fix-name [edn-schema]
  (update edn-schema :name edn-name-kw->avro-name))

(defn fix-alias [alias-kw]
  (edn-name-kw->avro-name alias-kw))

(defn fix-aliases [edn-schema]
  (if (contains? edn-schema :aliases)
    (update edn-schema :aliases #(map fix-alias %))
    edn-schema))

(defmulti fix-default avro-type-dispatch)

(defmethod fix-default :fixed
  [field-schema default]
  (byte-array->byte-str default))

(defmethod fix-default :bytes
  [field-schema default]
  (byte-array->byte-str default))

(defmethod fix-default :enum
  [field-schema default]
  (-> (name default)
      (csk/->SCREAMING_SNAKE_CASE)))

(defmethod fix-default :record
  [default-schema default-record]
  (reduce (fn [acc field]
            (let [{field-name :name
                   field-type :type
                   field-default :default} field]
              (assoc acc field-name (fix-default field-type field-default))))
          {} (:fields default-schema)))

(defmethod fix-default :array
  [field-schema default]
  (let [child-schema (:items field-schema)]
    (mapv #(fix-default child-schema %) default)))

(defmethod fix-default :map
  [field-schema default]
  (let [child-schema (:values field-schema)]
    (reduce-kv (fn [acc k v]
                 (assoc acc k (fix-default child-schema v)))
               {} default)))

(defmethod fix-default :union
  [field-schema default]
  ;; Union default's type must be the first type in the union
  (fix-default (first field-schema) default))

(defmethod fix-default :default
  [field-schema default]
  default)

(defn fix-fields [edn-schema]
  (update edn-schema :fields
          (fn [fields]
            (mapv (fn [field]
                    (let [field-type (:type field)
                          avro-type (get-avro-type field-type)]

                      (-> field
                          (update :name #(csk/->camelCase (name %)))
                          (update :type edn-schema->avro-schema)
                          (update :default #(fix-default field-type %)))))
                  fields))))

(defn fix-symbols [edn-schema]
  (update edn-schema :symbols #(mapv csk/->SCREAMING_SNAKE_CASE %)))

(defmethod edn-schema->avro-schema :record
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-fields)))

(defmethod edn-schema->avro-schema :enum
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)
      (fix-symbols)))

(defmethod edn-schema->avro-schema :fixed
  [edn-schema]
  (-> edn-schema
      (fix-name)
      (fix-aliases)))

(defmethod edn-schema->avro-schema :array
  [edn-schema]
  (update edn-schema :items edn-schema->avro-schema))

(defmethod edn-schema->avro-schema :map
  [edn-schema]
  (update edn-schema :values edn-schema->avro-schema))

(defmethod edn-schema->avro-schema :flex-map
  [edn-schema]
  (-> (flex-map-edn-schema->record-edn-schema edn-schema)
      (edn-schema->avro-schema)))

(defmethod edn-schema->avro-schema :union
  [edn-schema]
  (mapv edn-schema->avro-schema edn-schema))

(defmethod edn-schema->avro-schema :name-keyword
  [kw]
  (edn-name-kw->avro-name kw))

(defmethod edn-schema->avro-schema :default
  [edn-schema]
  edn-schema)

(defmethod make-default-data-size :null
  [edn-schema name->edn-schema]
  0)

(defmethod make-default-data-size :boolean
  [edn-schema name->edn-schema]
  1)

(defmethod make-default-data-size :int
  [edn-schema name->edn-schema]
  1) ;; Assume small numbers

(defmethod make-default-data-size :long
  [edn-schema name->edn-schema]
  8) ;; Assume large numbers

(defmethod make-default-data-size :float
  [edn-schema name->edn-schema]
  4)

(defmethod make-default-data-size :double
  [edn-schema name->edn-schema]
  8)

(defmethod make-default-data-size :bytes
  [edn-schema name->edn-schema]
  100) ;; SWAG

(defmethod make-default-data-size :string
  [edn-schema name->edn-schema]
  100) ;; SWAG

(defmethod make-default-data-size :record
  [edn-schema name->edn-schema]
  (reduce (fn [acc field]
            (+ (int acc)
               (int (make-default-data-size (:type field) name->edn-schema))))
          0 (:fields edn-schema)))

(defmethod make-default-data-size :enum
  [edn-schema name->edn-schema]
  ;; Return max number of bytes
  (Math/ceil (/ (count (:symbols edn-schema)) 128)))

(defmethod make-default-data-size :fixed
  [edn-schema name->edn-schema]
  (:size edn-schema))

(defmethod make-default-data-size :array
  [edn-schema name->edn-schema]
  (* 10 (int (make-default-data-size (:items edn-schema) name->edn-schema))))

(defmethod make-default-data-size :map
  [edn-schema name->edn-schema]
  (* 10 (+ 10 ;; key
           (int (make-default-data-size (:values edn-schema)
                                        name->edn-schema)))))

(defmethod make-default-data-size :flex-map
  [edn-schema name->edn-schema]
  (let [{:keys [keys values]} edn-schema]
    (* 10 (+ (int (make-default-data-size keys name->edn-schema))
             (int (make-default-data-size values name->edn-schema))))))

(defmethod make-default-data-size :union
  [edn-schema name->edn-schema]
  (apply clojure.core/max (map #(make-default-data-size % name->edn-schema)
                               edn-schema)))

(defmethod make-default-data-size :name-keyword
  [name-kw name->edn-schema]
  100) ;; Possibly recursive schema, so just return a reasonable default

(defn make-record-field [field]
  (when-not (#{2 3} (count field))
    (throw
     (ex-info (str "Record field definition must have 2 or 3 parameters. ("
                   "[field-name field-schema] or "
                   "[field-name field-schema field-default]).\n"
                   "  Got " (count field) " parameters.\n"
                   "  Bad field definition: " field)
              {:bad-field-def field})))
  (let [[field-name field-schema field-default] field
        field-edn-schema (if (satisfies? ILancasterSchema field-schema)
                           (edn-schema field-schema)
                           field-schema)]
    (when-not (keyword? field-name)
      (throw
       (ex-info (str "Field names must be keywords. Bad field name: "
                     field-name)
                (sym-map field-name field-schema field-default field))))
    {:name field-name
     :type field-edn-schema
     :default (default-data field-edn-schema field-default)}))

(defmethod make-edn-schema :record
  [schema-type name-kw fields]
  (let [name-kw (qualify-name-kw name-kw)
        fields (binding [**enclosing-namespace** (namespace name-kw)]
                 (mapv make-record-field fields))
        edn-schema {:name name-kw
                    :type :record
                    :fields fields}]
    edn-schema))

(defmethod make-edn-schema :enum
  [schema-type name-kw symbols]
  (let [name-kw (qualify-name-kw name-kw)
        edn-schema {:name name-kw
                    :type :enum
                    :symbols symbols}]
    edn-schema))

(defmethod make-edn-schema :fixed
  [schema-type name-kw size]
  (let [name-kw (qualify-name-kw name-kw)
        edn-schema {:name name-kw
                    :type :fixed
                    :size size}]
    edn-schema))

(defmethod make-edn-schema :array
  [schema-type name-kw items]
  {:type :array
   :items (ensure-edn-schema items)})

(defmethod make-edn-schema :map
  [schema-type name-kw values]
  {:type :map
   :values (ensure-edn-schema values)})

(defmethod make-edn-schema :flex-map
  [schema-type name-kw [ks vs]]
  {:name name-kw
   :type :flex-map
   :keys  (ensure-edn-schema ks)
   :values (ensure-edn-schema vs)})

(defmethod make-edn-schema :union
  [schema-type name-kw member-schemas]
  (mapv ensure-edn-schema member-schemas))

(defn avro-schema-type-dispatch [avro-schema]
  (cond
    (map? avro-schema)
    (let [{:keys [type]} avro-schema
          type-kw (keyword type)]
      (if (avro-primitive-types type-kw)
        :primitive
        type-kw))

    (string? avro-schema)
    (if (avro-primitive-type-strings avro-schema)
      :primitive
      :name-string)

    (sequential? avro-schema)
    :union))

(defn avro-name-str->edn-name-kw [name-str]
  (if (fullname? name-str)
    (let [ns (java-namespace->clj-namespace (fullname->ns name-str))
          name (csk/->kebab-case (fullname->name name-str))]
      (keyword ns name))
    (csk/->kebab-case-keyword name-str)))

(defn avro-name->edn-name [schema]
  (let [schema-name-str (:name schema)]
    (if-not (fullname? schema-name-str)
      (assoc schema :name (csk/->kebab-case-keyword schema-name-str))
      (let [schema-ns (-> (fullname->ns schema-name-str)
                          (java-namespace->clj-namespace))
            schema-name (-> (fullname->name schema-name-str)
                            (csk/->kebab-case))]
        (assoc schema :name (keyword schema-ns schema-name))))))

(defmulti ensure-defaults avro-type-dispatch)

(defmethod ensure-defaults :default
  [edn-schema name->edn-schema]
  edn-schema)

(defmethod ensure-defaults :array
  [edn-schema name->edn-schema]
  (update edn-schema :items #(ensure-defaults % name->edn-schema)))

(defmethod ensure-defaults :map
  [edn-schema name->edn-schema]
  (update edn-schema :values #(ensure-defaults % name->edn-schema)))

(defmethod ensure-defaults :union
  [edn-schema name->edn-schema]
  (mapv #(ensure-defaults % name->edn-schema) edn-schema))

(defmethod ensure-defaults :record
  [edn-schema name->edn-schema]
  (update edn-schema :fields
          (fn [fields]
            (mapv
             (fn [field]
               (let [{:keys [type default]} field
                     new-default (or default
                                     (default-data type nil name->edn-schema))
                     new-type (ensure-defaults type name->edn-schema)
                     field-type (get-avro-type type)
                     new-field (-> field
                                   (assoc :default new-default)
                                   (assoc :type new-type))]
                 new-field))
             fields))))

(defmulti avro-schema->edn-schema avro-schema-type-dispatch)

(defmethod avro-schema->edn-schema :primitive
  [avro-schema]
  (cond
    (map? avro-schema) (keyword (:type avro-schema))
    (string? avro-schema) (keyword avro-schema)
    :else (throw (ex-info (str "Unknown primitive schema: " avro-schema)
                          (sym-map avro-schema)))))

(defmethod avro-schema->edn-schema :name-string
  [avro-schema]
  (avro-name-str->edn-name-kw avro-schema))

(defmethod avro-schema->edn-schema :array
  [avro-schema]
  (-> avro-schema
      (update :type keyword)
      (update :items avro-schema->edn-schema)))

(defmethod avro-schema->edn-schema :map
  [avro-schema]
  (-> avro-schema
      (update :type keyword)
      (update :values avro-schema->edn-schema)))

(defmethod avro-schema->edn-schema :enum
  [avro-schema]
  (-> (avro-name->edn-name avro-schema)
      (update :type keyword)
      (update :symbols #(mapv csk/->kebab-case-keyword %))))

(defmethod avro-schema->edn-schema :fixed
  [avro-schema]
  (-> (avro-name->edn-name avro-schema)
      (update :type keyword)))

(defn avro-field->edn-field [field]
  (-> field
      (update :type avro-schema->edn-schema)
      (update :name avro-name-str->edn-name-kw)))

(defmethod avro-schema->edn-schema :record
  [avro-schema]
  (-> (avro-name->edn-name avro-schema)
      (update :type keyword)
      (update :fields #(mapv avro-field->edn-field %))))

(defmethod avro-schema->edn-schema :union
  [avro-union-schema]
  (mapv avro-schema->edn-schema avro-union-schema))
