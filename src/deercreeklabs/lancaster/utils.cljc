(ns deercreeklabs.lancaster.utils
  (:refer-clojure :exclude [long])
  (:require
   [camel-snake-kebab.core :as csk]
   #?(:clj [cheshire.core :as json])
   [clojure.pprint :as pprint]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   #?(:cljs [goog.math :as gm])
   #?(:clj [clj-commons.primitive-math :as pm])
   #?(:clj [puget.printer :refer [cprint-str]])
   [schema.core :as s])
  #?(:cljs
     (:require-macros
      [deercreeklabs.lancaster.utils :refer [sym-map]]))
  #?(:cljs
     (:import
      (goog.math Long))))

#?(:cljs (def class type))

#?(:cljs (def max-int (Long.fromInt 2147483647)))
#?(:cljs (def min-int (Long.fromInt -2147483648)))

#?(:clj (pm/use-primitive-operators))

(defonce *__INTERNAL__edn-schema->schema (atom {}))

(declare default-data edn-schemas-match?)

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
  (deserialize [this writer-pcf is opts])
  (edn-schema [this])
  (json-schema [this])
  (parsing-canonical-form [this])
  (fingerprint64 [this])
  (fingerprint128 [this])
  (fingerprint256 [this])
  (plumatic-schema [this])
  (child-schema [this] [this field-name-kw]))

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
(def avro-container-types #{:record :array :map :union})
(def avro-map-types #{:record :map})
(def avro-named-types #{:record :fixed :enum})
(def avro-numeric-types #{:int :long :float :double})
(def avro-byte-types #{:bytes :fixed})
(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-primitive-type-strings (into #{} (map name avro-primitive-types)))
(def Nil (s/eq nil))

(defn pprint-str [x]
  (with-out-str (pprint/pprint x)))

(defn pprint [x]
  #?(:clj (.write *out* (str (cprint-str x) "\n"))
     :cljs (pprint/pprint (str x "\n"))))

(s/defn ex-msg :- s/Str
  [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(s/defn ex-stacktrace :- s/Str
  [e]
  #?(:clj (str/join "\n" (map str (.getStackTrace ^Exception e)))
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

(defn path-key? [k]
  (or (keyword? k)
      (string? k)
      (integer? k)
      (ba/byte-array? k)))

(defn path? [x]
  (and (sequential? x)
       (reduce (fn [acc k]
                 (if (path-key? k)
                   acc
                   (reduced false)))
               true x)))

(defn schema-name [clj-name]
  (-> (name clj-name)
      (str/split #"-schema")
      (first)))

(defn fullname? [x]
  (cond
    (string? x) (str/includes? x ".")
    (keyword? x) (fullname? (name x))))

(defn fullname->ns [fullname]
  (let [parts (str/split fullname #"\.")
        ns (str/join "." (butlast parts))]
    (when-not (empty? ns)
      ns)))

(defn fullname->name [fullname]
  (-> (str/split fullname #"\.")
      (last)))

(defn qualify-name-kw [name-kw edn-schema]
  (let [{:keys [namespace]} edn-schema]
    (cond
      (qualified-keyword? name-kw)
      name-kw

      (not (keyword? name-kw))
      (throw (ex-info (str "Argument to qualify-name-kw (" name-kw
                           ") is not a keyword")
                      (sym-map name-kw)))

      namespace
      (keyword (name namespace) (name name-kw))

      **enclosing-namespace**
      (keyword (name **enclosing-namespace**) (name name-kw))

      :else
      name-kw)))

(defn name-keyword? [x]
  (and (keyword? x)
       (not (avro-primitive-types x))))

(defn clj-namespace->java-namespace [ns]
  (when ns
    (str/replace (str ns) #"-" "_")))

(defn java-namespace->clj-namespace [ns]
  (when ns
    (str/replace (str ns) #"_" "-")))

(defn edn-name-kw->avro-name [kw]
  (let [kw-ns (clj-namespace->java-namespace (namespace kw))
        kw-name (csk/->PascalCase (name kw))]
    (if kw-ns
      (str kw-ns "." kw-name)
      kw-name)))

(s/defn get-avro-type :- s/Keyword
  ([edn-schema]
   (get-avro-type edn-schema nil))
  ([edn-schema name->edn-schema]
   (let [at (cond
              (map? edn-schema) (:type edn-schema)
              (sequential? edn-schema) :union
              (avro-primitive-types edn-schema) edn-schema
              (keyword? edn-schema) :name-keyword
              (string? edn-schema) :name-string ;; For Avro schemas
              (nil? edn-schema) (throw
                                 (ex-info (str "Schema argument to "
                                               "get-avro-type is nil.")
                                          {:type :illegal-schema
                                           :subtype :schema-is-nil
                                           :schema edn-schema}))
              :else (throw (ex-info (str "Failed to get avro type for schema: "
                                         edn-schema)
                                    (sym-map edn-schema))))]
     (if (and (= :name-keyword at)
              name->edn-schema)
       (let [qn (qualify-name-kw edn-schema nil)
             es (name->edn-schema qn)]
         (get-avro-type es))
       at))))

(defn strip-lt-attrs [edn-schema]
  (dissoc edn-schema :logical-type :lt->avro :avro->lt :lt? :default-data
          :valid-k? :k->child-edn-schema :edn-sub-schemas :plumatic-schema))

(s/defn name-kw->name-str :- s/Str
  [kw :- s/Keyword]
  (cond
    (simple-keyword? kw) (name kw)
    (qualified-keyword? kw) (str (namespace kw) "." (name kw))
    :else (throw (ex-info (str "Argument (" kw ") is not a keyword.")
                          {:arg kw}))))

(defn named-type->name-kw [edn-schema]
  ;; https://avro.apache.org/docs/current/spec.html#names
  (let [{ns* :namespace
         name* :name} edn-schema
        name-ns (namespace name*)
        name-name (name name*)]
    (when-not name*
      (throw (ex-info (str "No `:name` attribute found.")
                      (sym-map edn-schema))))
    (cond
      name-ns
      name*

      (not (str/blank? ns*))
      (keyword ns* name-name)

      :else
      ;; Unqualified name given; we need to qualify it
      (keyword **enclosing-namespace** name-name))))

(defn named-edn-schema->name-kw [edn-schema]
  (cond
    (avro-named-types (:type edn-schema))
    (named-type->name-kw edn-schema)

    (avro-primitive-types edn-schema)
    edn-schema

    (qualified-keyword? edn-schema)
    edn-schema

    (keyword? edn-schema)
    (let [ns (or (:namespace edn-schema)
                 **enclosing-namespace**)]
      (keyword (name ns) (name edn-schema)))

    :else nil))

(s/defn edn-schema->name-kw :- s/Keyword
  [edn-schema]
  (cond
    (avro-named-types (:type edn-schema))
    (named-type->name-kw edn-schema)

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

(defn match-exception? [e]
  (let [msg (ex-msg e)]
    (or (str/includes? msg "do not match")
        (str/includes? msg "No schemas in reader union schema match writer.")
        (re-find #"No method in multimethod.*make-deserializer" msg))))

(defn make-default-fixed-or-bytes [num-bytes default]
  (byte-array->byte-str (or default
                            (ba/byte-array (take num-bytes (repeat 0))))))

(defn make-default-record [record-edn-schema default-record name->edn-schema]
  (let [{:keys [fields name]} record-edn-schema]
    (reduce (fn [acc field]
              (let [{field-name :name
                     field-type :type
                     field-default* :default} field
                    field-default (or field-default*
                                      (default-data field-type nil
                                                    name->edn-schema))
                    field-schema (ensure-edn-schema field-type)
                    v (if default-record
                        (default-data field-schema
                                      (default-record field-name)
                                      name->edn-schema)
                        field-default)]
                (assoc acc field-name v)))
            {} fields)))

(defn default-data
  ([edn-schema]
   (default-data edn-schema nil {}))
  ([edn-schema field-default]
   (default-data edn-schema field-default {}))
  ([edn-schema field-default name->edn-schema]
   (if (:logical-type edn-schema)
     (:default-data edn-schema)
     (let [avro-type (get-avro-type edn-schema)]
       (case avro-type
         :record (make-default-record edn-schema field-default name->edn-schema)
         :union (default-data (first edn-schema) field-default name->edn-schema)
         :fixed (make-default-fixed-or-bytes (:size edn-schema) field-default)
         :bytes (make-default-fixed-or-bytes 0 field-default)
         :null nil
         (or field-default
             (case avro-type
               :boolean false
               :int (int -1)
               :long -1
               :float (float -1.0)
               :double (double -1.0)
               :string ""
               :enum (first (:symbols edn-schema))
               :array []
               :map {}
               :name-keyword (default-data (name->edn-schema edn-schema)
                                           field-default
                                           name->edn-schema))))))))

(defn first-arg-dispatch [first-arg & rest-of-args]
  first-arg)

(defn avro-type-dispatch-lt [edn-schema & args]
  (let [avro-type (get-avro-type edn-schema)]
    (if (:logical-type edn-schema)
      :logical-type
      avro-type)))

(defn avro-type-dispatch [edn-schema & args]
  (get-avro-type edn-schema))

(defmulti make-serializer avro-type-dispatch-lt)
(defmulti edn-schema->avro-schema avro-type-dispatch-lt)
(defmulti edn-schema->plumatic-schema avro-type-dispatch-lt)
(defmulti make-default-data-size avro-type-dispatch)

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
  (and (long-or-int? data)
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

(s/defn long= :- s/Bool
  [a :- s/Any
   b :- s/Any]
  #?(:clj (= a b)
     :cljs (when (and a b)
             (cond
               (long? a) (.equals ^Long a b)
               (long? b) (.equals ^Long b a)
               :else (= a b)))))

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
     :cljs (.fromBits ^Long Long (int low) (int high))))

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
     :cljs (.fromString ^Long Long s)))

(defn long->str [^long l]
  (when-not (long? l)
    (throw (ex-info (str "Argument to long->str is not a long. Got`" l "`.")
                    {:given-arg l})))
  #?(:cljs (.toString l)
     :clj (Long/toString l 10)))

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

(defn int->long [n]
  #?(:clj (clojure.core/long n)
     :cljs (.fromInt ^Long Long n)))

(defn more-than-one? [schema-set edn-schemas]
  (> (count (keep #(schema-set (get-avro-type %)) edn-schemas))
     1))

(defn contains-union? [edn-schemas]
  (some #(= :union (get-avro-type %)) edn-schemas))

#?(:cljs
   (defn write-long-varint-zz-long [output-stream ^Long l]
     (let [zz-n (.xor (.shiftLeft l 1)
                      (.shiftRight l 63))]
       (loop [^Long n zz-n]
         (if (.isZero ^Long (.and n (Long.fromInt -128)))
           (let [b (.and n (Long.fromInt 127))]
             (write-byte output-stream b))
           (let [b (-> (.and n (Long.fromInt 127))
                       (.or (Long.fromInt 128)))]
             (write-byte output-stream b)
             (recur ^Long (.shiftRightUnsigned n 7))))))))

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
            ^Long out ^Long (.getZero Long)]
       (let [^Long b ^Long (.fromNumber Long (read-byte input-stream))
             ^Long x128 ^Long (.fromInt Long 128)]
         (if (.isZero ^Long (.and b x128))
           (let [^Long zz-n (-> (.shiftLeft b i)
                                (.or out))
                 zero ^Long (.getZero ^Long Long)
                 one ^Long (.getOne ^Long Long)
                 long-out (.xor ^Long (.shiftRightUnsigned zz-n 1)
                                ^Long (.subtract zero
                                                 ^Long (.and zz-n one)))]
             long-out)
           (let [out (-> (.and b (.fromInt ^Long Long 127))
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
                   expected ". Path: " path ".")
              (-> (sym-map expected data type path edn-schema)
                  (assoc :exception-type :invalid-data))))))

(defn throw-non-string-map-key [k v edn-schema data path]
  (let [display-k (if (nil? k)
                    "nil"
                    k)
        type #?(:clj (if (nil? k)
                       "nil"
                       (.toString ^Class (class data)))
                :cljs (goog/typeOf data))]
    (throw
     (ex-info (str "Map key `" display-k "` (type: " type ") is not a valid "
                   "string. All map keys must be strings. Path: " path ".")
              (sym-map k v data type path edn-schema)))))

(defn throw-non-string-set-element [k edn-schema data path]
  (let [display-k (if (nil? k)
                    "nil"
                    k)
        type #?(:clj (if (nil? k)
                       "nil"
                       (.toString ^Class (class data)))
                :cljs (goog/typeOf data))]
    (throw
     (ex-info (str "Set element `" display-k "` (type: " type ") is not a "
                   "valid string. All set elements must be strings. Path: "
                   path ".")
              (sym-map k data type path edn-schema)))))

(defn edn->json-string [edn]
  #?(:clj (json/generate-string edn)
     :cljs (js/JSON.stringify (clj->js edn))))

(defn json-schema->avro-schema [json-str]
  #?(:clj (json/parse-string json-str true)
     :cljs (js->clj (js/JSON.parse json-str) :keywordize-keys true)))

(defn swap-named-value!
  [*atom edn-schema value]
  (let [name-kw (named-edn-schema->name-kw edn-schema)]
    (swap! *atom assoc name-kw value)))

(defmethod make-serializer :null
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (nil? data)
      (throw-invalid-data-error edn-schema data path))))

(defmethod make-serializer :boolean
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (boolean? data)
      (throw
       (throw-invalid-data-error edn-schema data path)))
    (write-byte os (if data 1 0))))

(defmethod make-serializer :int
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-int? data)
      (throw-invalid-data-error edn-schema data path))
    (write-long-varint-zz os data)))

(defmethod make-serializer :long
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-long? data)
      (throw-invalid-data-error edn-schema data path))
    (write-long-varint-zz os data)))

(defmethod make-serializer :float
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-float? data)
      (throw-invalid-data-error edn-schema data path))
    (write-float os data)))

(defmethod make-serializer :double
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (valid-double? data)
      (throw-invalid-data-error edn-schema data path))
    (write-double os data)))

(defmethod make-serializer :string
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (string? data)
      (throw-invalid-data-error edn-schema data path))
    (write-utf8-string os data)))

(defmethod make-serializer :bytes
  [edn-schema name->edn-schema *name->serializer]
  (fn serialize [os data path]
    (when-not (ba/byte-array? data)
      (throw-invalid-data-error edn-schema data path))
    (write-bytes-w-len-prefix os data)))

(defn throw-bad-enum-data [data path symbols edn-schema]
  (throw
   (let [display-data (if (nil? data) "nil" data)]
     (ex-info
      (str "Data `" display-data "` is not one of the "
           "symbols of this enum `" symbols "`. Path: " path)
      (sym-map data path symbols edn-schema)))))

(defmethod make-serializer :enum
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [name symbols]} edn-schema
        symbol->index (apply hash-map
                             (apply concat (map-indexed
                                            #(vector %2 %1) symbols)))
        ser (fn serialize [os data path]
              (if-let [i (symbol->index data)]
                (write-long-varint-zz os i)
                (if-not (keyword? data)
                  (throw
                   (ex-info (str "Enum data must be a keyword. Got: " data)
                            (sym-map data)))
                  (throw-bad-enum-data data path symbols edn-schema))))]
    (swap-named-value! *name->serializer edn-schema ser)
    ser))

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
    (swap-named-value! *name->serializer edn-schema serializer)
    serializer))

(defn make-serialize-set
  "Implemented as an Avro map w/ null values."
  [edn-schema]
  (fn [os data path]
    (when-not (set? data)
      (let [display-data (if (nil? data)
                           "nil"
                           data)
            type #?(:clj (if (nil? data)
                           "nil"
                           (.toString ^Class (class data)))
                    :cljs (goog/typeOf data))]
        (throw
         (ex-info
          (str "Data `" display-data "` (type: " type ") is not a valid "
               "Clojure set. Path: " path)
          (sym-map data type path edn-schema)))))
    (when (pos? (count data))
      (write-long-varint-zz os (count data))
      (doseq [k data]
        (when-not (string? k)
          (throw-non-string-set-element k edn-schema data path))
        (write-utf8-string os k)))
    (write-byte os 0)))

(defmethod make-serializer :map
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [values]} edn-schema
        serialize-value (make-serializer values name->edn-schema
                                         *name->serializer)]
    (if (= :null values)
      (make-serialize-set edn-schema)
      (fn serialize [os data path]
        (when-not (valid-map? data)
          (throw-invalid-data-error edn-schema data path))
        (when (pos? (count data))
          (write-long-varint-zz os (count data))
          (doseq [[k v] data]
            (let [child-path (conj path k)]
              (when-not (string? k)
                (throw-non-string-map-key k v edn-schema data child-path))
              (write-utf8-string os k)
              (serialize-value os v child-path))))
        (write-byte os 0)))))

(defmethod make-serializer :logical-type
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [lt->avro logical-type]} edn-schema
        _ (when-not lt->avro
            (throw (ex-info (str "Logical type `" logical-type "` is missing "
                                 "a `lt->avro` attribute.")
                            (sym-map logical-type edn-schema))))
        non-lt-ser (make-serializer (strip-lt-attrs edn-schema)
                                    name->edn-schema *name->serializer)
        lt-ser (fn serialize [os data path]
                 (non-lt-ser os (lt->avro data) path))]
    ;; Store the lt serializer, overwriting the non-lt serializer
    (swap-named-value! *name->serializer edn-schema lt-ser)
    lt-ser))

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

(defmulti make-union-serializer
  (fn [edn-schema name->edn-schema *name->serializer]
    (if (= 2 (count edn-schema))
      :two-schemas
      :any-number-of-schemas)))

(defn throw-invalid-union-data-error [union-edn-schema data path]
  (let [expected (get-avro-type union-edn-schema)
        display-data (if (nil? data)
                       "nil"
                       data)
        display-type #?(:clj (if (nil? data)
                               "nil"
                               (.toString ^Class (class data)))
                        :cljs (goog/typeOf data))]
    (throw
     (ex-info
      (str "Data `" display-data "` (type: " display-type ") does "
           "not match any schema in the given union schema. Path: "
           path ". Union edn-schema: `" union-edn-schema "`.")
      (sym-map data display-data display-type path union-edn-schema)))))

;; We don't just use `make-union-with-x-serializer` here because this
;; is a very heavily used schema and we can optimize it slightly. The `:null`
;; serializer is a no-op, so we exploit that by skipping the call.
(defn make-union-with-null-serializer
  [union-edn-schema name->edn-schema *name->serializer]
  (let [[null-branch other-branch] (if (= :null (get-avro-type
                                                 (first union-edn-schema)
                                                 name->edn-schema))
                                     [0 1]
                                     [1 0])
        other-serializer (make-serializer (nth union-edn-schema other-branch)
                                          name->edn-schema
                                          *name->serializer)]
    (fn serialize [os data path]
      (if (nil? data)
        (write-long-varint-zz os null-branch)
        (try
          (write-long-varint-zz os other-branch)
          (other-serializer os data path)
          (catch #?(:clj Exception :cljs js/Error) e
            (let [{:keys [exception-type]} (ex-data e)]
              (if (not= :invalid-data exception-type)
                (throw e)
                (throw-invalid-union-data-error
                 union-edn-schema data path)))))))))

(defn make-union-with-x-serializer
  [union-edn-schema x-types pred name->edn-schema *name->serializer]
  (let [[x-branch other-branch] (if (x-types (get-avro-type
                                              (first union-edn-schema)
                                              name->edn-schema))
                                  [0 1]
                                  [1 0])
        x-serializer (make-serializer (nth union-edn-schema x-branch)
                                      name->edn-schema
                                      *name->serializer)
        other-serializer (make-serializer (nth union-edn-schema other-branch)
                                          name->edn-schema
                                          *name->serializer)]
    (fn serialize [os data path]
      (if (pred data)
        (do
          (write-long-varint-zz os x-branch)
          (x-serializer os data path))
        (try
          (write-long-varint-zz os other-branch)
          (other-serializer os data path)
          (catch #?(:clj Exception :cljs js/Error) e
            (let [{:keys [exception-type]} (ex-data e)]
              (if (not= :invalid-data exception-type)
                (throw e)
                (throw-invalid-union-data-error
                 union-edn-schema data path)))))))))

(defn make-union-with-two-enums-serializer
  [union-edn-schema name->edn-schema *name->serializer]
  (let [[kws-0 kws-1] (map #(-> % :symbols set) union-edn-schema)
        overlapping-kws (set/intersection kws-0 kws-1)
        _ (when (seq overlapping-kws)
            (throw (ex-info (str "Enum symbols in unions cannot overlap. The "
                                 "symbols `" overlapping-kws "` appear in more "
                                 "than one schema in the union.")
                            (sym-map union-edn-schema overlapping-kws))))
        serializer-0 (make-serializer (first union-edn-schema)
                                      name->edn-schema
                                      *name->serializer)
        serializer-1 (make-serializer (second union-edn-schema)
                                      name->edn-schema
                                      *name->serializer)]
    (fn serialize [os data path]
      (cond
        (kws-0 data)
        (do
          (write-long-varint-zz os 0)
          (serializer-0 os data path))

        (kws-1 data)
        (do
          (write-long-varint-zz os 1)
          (serializer-1 os data path))

        :else
        (throw-invalid-union-data-error union-edn-schema data path)))))

(defn make-field-info
  [record-edn-schema field-edn-schema name->edn-schema *name->serializer]
  (let [{:keys [type]} field-edn-schema
        name-kw (named-edn-schema->name-kw type)
        serializer (or (@*name->serializer name-kw)
                       (make-serializer
                        type name->edn-schema *name->serializer))
        k (qualify-name-kw (:name field-edn-schema) record-edn-schema)
        unq-k (keyword (name k))
        nilable? (or (= :null type)
                     (and (sequential? type)
                          ((set type) :null)))]
    (sym-map k unq-k serializer nilable?)))

(defn get-record-field-keyset [edn-schema* name->edn-schema]
  (let [edn-schema (if-not (keyword? edn-schema*)
                     edn-schema*
                     (name->edn-schema (qualify-name-kw edn-schema* nil)))]
    (binding [**enclosing-namespace** (or (namespace (:name edn-schema))
                                          (:namespace edn-schema))]
      (reduce (fn [acc field]
                (let [k (qualify-name-kw (:name field) edn-schema)
                      unq-k (keyword (name k))]
                  (-> acc
                      (conj k)
                      (conj unq-k))))
              #{}
              (:fields edn-schema)))))

(defn get-union-record-keyset-info
  [union-edn-schema name->edn-schema]
  (let [keysets (map (fn [edn-schema]
                       (if (= :record
                              (get-avro-type edn-schema name->edn-schema))
                         (get-record-field-keyset edn-schema name->edn-schema)
                         #{}))
                     union-edn-schema)]
    {:overlapping-record-keyset (apply set/intersection keysets)
     :branch-keysets keysets}))

(defn throw-name-required-error [data overlapping-ks path]
  (throw (ex-info
          (str "Missing `:deercreeklabs.lancaster/record-name` value in record "
               "data. This is required to disambiguate between records in this "
               "union schema, since the records have overlapping field names.")
          (sym-map overlapping-ks data path))))

(defn throw-unknown-name-error [data given-name valid-names path]
  (throw (ex-info
          (str "The given record name `" given-name "` is not a valid name for "
               "any of the records in this union.")
          (sym-map data given-name valid-names path))))

(defn throw-ambiguous-empty-data-error [{:keys [data union-edn-schema path]}]
  (throw (ex-info
          (str "Can't serialize data `{}` because it matches multiple "
               "record schemas in the union schema. Add a `"
               ":deercreeklabs.lancaster/record-name` key to "
               "disambiguate which record this data represents.")
          (sym-map data union-edn-schema path))))

(defn rec-edn-schema->names-set [edn-schema]
  (let [q-name-kw (qualify-name-kw (:name edn-schema) edn-schema)
        unq-name-kw (keyword (name q-name-kw))
        q-name-str (name-kw->name-str q-name-kw)
        unq-name-str (name-kw->name-str unq-name-kw)]
    (-> #{q-name-kw}
        (conj unq-name-kw)
        (conj q-name-str)
        (conj unq-name-str))))

(defn make-union-with-two-records-serializer
  [union-edn-schema name->edn-schema *name->serializer]
  (let [info (get-union-record-keyset-info
              union-edn-schema name->edn-schema)
        {:keys [branch-keysets overlapping-record-keyset]} info
        serializers (map #(make-serializer %
                                           name->edn-schema
                                           *name->serializer)
                         union-edn-schema)
        branch-range (range (count union-edn-schema))
        name->branch (reduce (fn [acc branch]
                               (let [sch (nth union-edn-schema branch)
                                     names-set (rec-edn-schema->names-set sch)]
                                 (reduce (fn [acc* name*]
                                           (assoc acc* name* branch))
                                         acc
                                         names-set)))
                             {}
                             branch-range)]
    (if (seq overlapping-record-keyset)
      ;; Overlapping keys
      (fn serialize [os data path]
        (let [record-name (:deercreeklabs.lancaster/record-name data)
              branch (get name->branch record-name)
              serializer (when branch (nth serializers branch))]
          (when-not record-name
            (throw-name-required-error data overlapping-record-keyset path))
          (when-not branch
            (throw-unknown-name-error
             data record-name (keys name->branch) path))
          (write-long-varint-zz os branch)
          (serializer os data path)))
      ;; No overlapping keys
      (fn serialize [os data path]
        (let [data-ks (set (keys data))
              record-name (:deercreeklabs.lancaster/record-name data)
              branch (if record-name
                       (let [b (name->branch record-name)]
                         (when-not b
                           (throw-unknown-name-error
                            data record-name (keys name->branch) path))
                         b)
                       (let [data-ks (set (keys data))]
                         (reduce
                          (fn [acc b]
                            (if (seq (set/intersection (nth branch-keysets b)
                                                       data-ks))
                              (reduced b)
                              (when (= {} data)
                                (throw-ambiguous-empty-data-error
                                 (sym-map data union-edn-schema path)))))
                          nil
                          [0 1])))]
          (if-not branch
            (throw-invalid-union-data-error union-edn-schema data path)
            (let [serializer (nth serializers branch)]
              (write-long-varint-zz os branch)
              (serializer os data path))))))))

(defn make-union-of-map-and-record-serializer
  [union-edn-schema name->edn-schema *name->serializer]
  (let [[map-branch record-branch] (if (= :map (get-avro-type
                                                (first union-edn-schema)
                                                name->edn-schema))
                                     [0 1]
                                     [1 0])
        map-serializer (make-serializer (nth union-edn-schema map-branch)
                                        name->edn-schema
                                        *name->serializer)
        record-serializer (make-serializer (nth union-edn-schema record-branch)
                                           name->edn-schema
                                           *name->serializer)]
    (fn serialize [os data path]
      (if (and (map? data)
               (-> data first first string?)) ; Avro maps have string keys
        (do
          (write-long-varint-zz os map-branch)
          (map-serializer os data path))
        (try
          (write-long-varint-zz os record-branch)
          (record-serializer os data path)
          (catch #?(:clj Exception :cljs js/Error) e
            (let [{:keys [exception-type]} (ex-data e)]
              (if (not= :invalid-data exception-type)
                (throw e)
                (throw-invalid-union-data-error
                 union-edn-schema data path)))))))))

(defmethod make-union-serializer :two-schemas
  [edn-schema name->edn-schema *name->serializer]
  (let [type-set (->> edn-schema
                      ;; Must use mapv here because `binding` doesn't work
                      ;; with laziness
                      (mapv #(get-avro-type % name->edn-schema))
                      (set))]
    (cond
      (type-set :null)
      (make-union-with-null-serializer
       edn-schema name->edn-schema *name->serializer)

      (type-set :boolean)
      (make-union-with-x-serializer
       edn-schema #{:boolean} boolean? name->edn-schema *name->serializer)

      (type-set :int)
      (make-union-with-x-serializer
       edn-schema #{:int} int? name->edn-schema *name->serializer)

      (type-set :long)
      (make-union-with-x-serializer
       edn-schema #{:long} long-or-int? name->edn-schema *name->serializer)

      (seq (set/intersection type-set #{:float :double}))
      (make-union-with-x-serializer
       edn-schema #{:float :double} number? name->edn-schema *name->serializer)

      (type-set :string)
      (make-union-with-x-serializer
       edn-schema #{:string} string? name->edn-schema *name->serializer)

      (seq (set/intersection type-set avro-byte-types))
      (make-union-with-x-serializer
       edn-schema avro-byte-types ba/byte-array? name->edn-schema
       *name->serializer)

      (type-set :array)
      (make-union-with-x-serializer
       edn-schema #{:array} sequential? name->edn-schema *name->serializer)

      (= #{:map :record} type-set)
      (make-union-of-map-and-record-serializer
       edn-schema name->edn-schema *name->serializer)

      (type-set :map)
      (make-union-with-x-serializer
       edn-schema #{:map} map? name->edn-schema *name->serializer)

      (and (type-set :enum)
           (= 2 (count type-set)))
      (make-union-with-x-serializer
       edn-schema #{:enum} keyword? name->edn-schema *name->serializer)

      (= #{:enum} type-set)
      (make-union-with-two-enums-serializer
       edn-schema name->edn-schema *name->serializer)

      (and (type-set :record)
           (= 2 (count type-set)))
      (make-union-with-x-serializer
       edn-schema #{:record} map? name->edn-schema *name->serializer)

      (= #{:record} type-set)
      (make-union-with-two-records-serializer
       edn-schema name->edn-schema *name->serializer)

      :else
      (throw (ex-info (str "Internal error: Unhandled union schema: `"
                           edn-schema "`.")
                      (sym-map edn-schema type-set))))))

(def avro-type->union-data-pred
  {:null nil?
   :boolean boolean?
   :int int?
   :long long-or-int?
   :float number?
   :double number?
   :bytes ba/byte-array?
   :string string?
   :enum keyword?
   :fixed ba/byte-array?
   :array sequential?
   :map map?
   :record map?})

(defn count-schema-types [type* union-edn-schema name->edn-schema]
  (->> union-edn-schema
       (filter #(= type* (get-avro-type % name->edn-schema)))
       (count)))

(defn make-multi-rec-non-overlapping-pred
  [{:keys [union-edn-schema branch-rec-keyset rec-name-set]}]
  (fn [data path]
    (if-let [rn (:deercreeklabs.lancaster/record-name data)]
      (rec-name-set rn)
      (if-not (map? data)
        false
        (or (seq (set/intersection
                  (set (keys data))
                  branch-rec-keyset))
            (when (= {} data)
              (throw-ambiguous-empty-data-error
               (sym-map data union-edn-schema path))))))))

(defn make-union-pred [arg]
  (let [{:keys [avro-type branch branch-keysets edn-schema num-enums num-maps
                num-recs overlapping-record-keyset union-edn-schema]} arg
        overlapping-recs? (boolean (seq overlapping-record-keyset))
        map-branch? (= :map avro-type)
        enum-branch? (= :enum avro-type)
        rec-branch? (= :record avro-type)
        enum-symbol-set (if enum-branch?
                          (-> edn-schema :symbols set)
                          #{})
        branch-rec-keyset (when rec-branch?
                            (nth branch-keysets branch))
        rec-name-set (when rec-branch?
                       (rec-edn-schema->names-set edn-schema))]
    (cond
      (and enum-branch? (> num-enums 1))
      (fn [data path]
        (enum-symbol-set data))

      (and map-branch? (pos? num-recs))
      (fn [data path]
        (and (map? data)
             (-> data first first string?)))

      (and rec-branch?
           (pos? num-maps)
           (= 1 num-recs))
      (fn [data path]
        (and (map? data)
             (-> data first first keyword?)))

      (and rec-branch?
           (> (int num-recs) 1)
           (not overlapping-recs?))
      (make-multi-rec-non-overlapping-pred
       (sym-map branch-rec-keyset union-edn-schema rec-name-set))

      (and rec-branch?
           (> (int num-recs) 1)
           overlapping-recs?)
      (fn [data path]
        (let [rn (:deercreeklabs.lancaster/record-name data)]
          (when-not rn
            (throw-name-required-error
             data overlapping-record-keyset path))
          (rec-name-set rn)))

      :else
      (let [pred* (avro-type->union-data-pred avro-type)]
        (fn [data path]
          (pred* data))))))

(defmethod make-union-serializer :any-number-of-schemas
  [union-edn-schema name->edn-schema *name->serializer]
  (let [branch->avro-types (mapv get-avro-type union-edn-schema name->edn-schema)
        num-enums (count-schema-types :enum union-edn-schema name->edn-schema)
        num-recs (count-schema-types :record union-edn-schema name->edn-schema)
        ;; Will only be 0 or 1. More than one map is prohibited by the spec.
        num-maps (count-schema-types :map union-edn-schema name->edn-schema)
        info (get-union-record-keyset-info
              union-edn-schema name->edn-schema)
        {:keys [branch-keysets overlapping-record-keyset]} info
        infos (reduce
               (fn [acc branch]
                 (let [edn-schema (nth union-edn-schema branch)
                       avro-type (get-avro-type edn-schema name->edn-schema)
                       enum-symbol-set (if (= :enum avro-type)
                                         (-> edn-schema :symbols set)
                                         #{})
                       pred (make-union-pred
                             (sym-map avro-type branch branch-keysets edn-schema
                                      num-enums num-maps num-recs
                                      overlapping-record-keyset
                                      union-edn-schema))
                       serializer (make-serializer edn-schema
                                                   name->edn-schema
                                                   *name->serializer)]
                   (conj acc (sym-map branch enum-symbol-set pred serializer))))
               []
               (range (count union-edn-schema)))
        overlapping-kws (apply set/intersection (map :enum-symbol-set infos))]
    (when (seq overlapping-kws)
      (throw (ex-info (str "Enum symbols in unions cannot overlap. The "
                           "symbols `" overlapping-kws "` appear in more "
                           "than one schema in the union.")
                      (sym-map union-edn-schema overlapping-kws))))
    (fn serialize [os data path]
      (let [info (reduce (fn [acc {:keys [pred] :as info}]
                           (when (pred data path)
                             (reduced info)))
                         nil
                         infos)
            {:keys [branch serializer]} info]
        (when-not info
          (throw-invalid-union-data-error edn-schema data path))
        (write-long-varint-zz os branch)
        (serializer os data path)))))

(defmethod make-serializer :union
  [edn-schema name->edn-schema *name->serializer]
  (make-union-serializer edn-schema name->edn-schema *name->serializer))

(defn throw-missing-key-error [k data path]
  (throw (ex-info
          (str "Record data is missing key `" k "`. Path: "
               path ". Data: " data ".")
          (sym-map k data path))))

(defn throw-non-nilable-value-error [k data path]
  (throw (ex-info
          (str "Record value for key `" k "` is nil, but field is non-nilable. "
               "Path: " path ". Data: " data ".")
          (sym-map k data path))))

(defmethod make-serializer :record
  [edn-schema name->edn-schema *name->serializer]
  (let [{:keys [fields name]} edn-schema
        name-kw (named-edn-schema->name-kw edn-schema)]
    (or (@*name->serializer name-kw)
        (let [schema-namespace (:namespace edn-schema)
              ens (or (namespace name) schema-namespace)
              field-infos (binding [**enclosing-namespace** ens]
                            ;; Must use mapv here because `binding` doesn't work
                            ;; with laziness
                            (mapv #(make-field-info edn-schema %
                                                    name->edn-schema
                                                    *name->serializer)
                                  fields))
              serializer (fn serialize [os data path]
                           (when-not (map? data)
                             (throw-invalid-data-error edn-schema data path))
                           (doseq [{:keys [k unq-k
                                           serializer nilable?]} field-infos]
                             (let [field-data (or (get data k)
                                                  (get data unq-k))]
                               (when (and (nil? field-data)
                                          (not nilable?))
                                 (if (or (contains? data k)
                                         (contains? data unq-k))
                                   (throw-non-nilable-value-error k data path)
                                   (throw-missing-key-error unq-k data path)))
                               (serializer os field-data (conj path k)))))]
          (swap-named-value! *name->serializer edn-schema serializer)
          serializer))))

(defmethod make-serializer :name-keyword
  [name-kw name->edn-schema *name->serializer]
  (let [edn-schema (name->edn-schema name-kw)
        qualified-name-kw (qualify-name-kw name-kw edn-schema)]
    (fn serialize [os data path]
      (if-let [serializer (@*name->serializer qualified-name-kw)]
        (serializer os data path)
        (throw (ex-info "Failed to find serializer for named type."
                        {:qualified-name qualified-name-kw
                         :name->serializer-keys (keys @*name->serializer)}))))))

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
  (let [{:keys [name symbols]} edn-schema]
    (apply s/enum symbols)))

(defmethod edn-schema->plumatic-schema :fixed
  [edn-schema name->edn-schema]
  StringOrBytes)

(defmethod edn-schema->plumatic-schema :array
  [edn-schema name->edn-schema]
  [(edn-schema->plumatic-schema (:items edn-schema) name->edn-schema)])

(defmethod edn-schema->plumatic-schema :map
  [edn-schema name->edn-schema]
  {s/Str (edn-schema->plumatic-schema (:values edn-schema) name->edn-schema)})

(defmethod edn-schema->plumatic-schema :logical-type
  [edn-schema name->edn-schema]
  (let [{:keys [plumatic-schema]} edn-schema]
    (or plumatic-schema s/Any)))

(defmethod edn-schema->plumatic-schema :record
  [edn-schema name->edn-schema]
  (let [record-name-kw (:name edn-schema)]
    (binding [**enclosing-namespace**
              (or (namespace record-name-kw)
                  (:namespace edn-schema))]
      (reduce (fn [acc {:keys [name type]}]
                (let [key-fn (if (and (= :union (get-avro-type type))
                                      (= :null (first type)))
                               s/optional-key
                               s/required-key)]
                  (assoc acc (key-fn name)
                         (edn-schema->plumatic-schema type name->edn-schema))))
              {s/Any s/Any} (:fields edn-schema)))))

(defmethod edn-schema->plumatic-schema :name-keyword
  [name-kw name->edn-schema]
  ;; Schemas are looser than optimal, due to recursion issues
  (if-let [edn-schema (name->edn-schema name-kw)]
    (case (get-avro-type edn-schema)
      :record {s/Any s/Any}
      :fixed StringOrBytes
      :enum s/Keyword)
    s/Any))

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
   nil (constantly true)})

(defn edn-schema->pred [edn-schema name->edn-schema]
  (-> edn-schema
      (get-avro-type name->edn-schema)
      (avro-type->pred)))

(defn edn-schema->pred-and-plumatic-schema [edn-schema name->edn-schema]
  (let [pred (edn-schema->pred edn-schema name->edn-schema)
        pschema (edn-schema->plumatic-schema edn-schema name->edn-schema)]
    [pred pschema]))

(defmethod edn-schema->plumatic-schema :union
  [edn-schema name->edn-schema]
  (let [pairs (reduce
               (fn [acc item-schema]
                 (let [[pred pschema] (edn-schema->pred-and-plumatic-schema
                                       item-schema name->edn-schema)]
                   (-> acc
                       (conj pred)
                       (conj pschema))))
               []
               edn-schema)]
    (apply s/conditional pairs)))

(defn get-schemas!
  [edn-schema *name->edn-schema]
  (let [avro-type (get-avro-type edn-schema)]
    (when (avro-named-types avro-type)
      (swap-named-value! *name->edn-schema
                         edn-schema
                         edn-schema))
    (let [child-schemas (case avro-type
                          :record (map :type (:fields edn-schema))
                          :array [(:items edn-schema)]
                          :map [(:values edn-schema)]
                          :union edn-schema
                          [])]
      (binding [**enclosing-namespace** (or (:namespace edn-schema)
                                            **enclosing-namespace**)]
        (doseq [child-schema child-schemas]
          (binding [**enclosing-namespace** (or (:namespace child-schema)
                                                **enclosing-namespace**)])
          (get-schemas! child-schema *name->edn-schema))))))

(defn make-name->edn-schema [edn-schema]
  (let [*name->edn-schema (atom (zipmap avro-primitive-types
                                        avro-primitive-types))]
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

(defn fix-field-name [field-name]
  (let [ns-part (namespace field-name)
        name-part (name field-name)]
    (csk/->camelCase (if ns-part
                       (str ns-part "-" name-part)
                       name-part))))

(defn fix-fields [edn-schema]
  (update edn-schema :fields
          (fn [fields]
            (mapv (fn [field]
                    (let [field-type (:type field)
                          avro-type (get-avro-type field-type)]

                      (-> field
                          (update :name fix-field-name)
                          (update :type edn-schema->avro-schema)
                          (update :default #(fix-default field-type %)))))
                  fields))))

(defn fix-symbols [edn-schema]
  (update edn-schema :symbols
          (fn [symbols]
            (mapv #(csk/->SCREAMING_SNAKE_CASE (name %))
                  symbols))))

(defmethod edn-schema->avro-schema :record
  [edn-schema]
  (-> (fix-name edn-schema)
      (fix-aliases)
      (fix-fields)))

(defmethod edn-schema->avro-schema :enum
  [edn-schema]
  (let [sch (-> (fix-name edn-schema)
                (fix-aliases)
                (fix-symbols)
                )]
    (if (:default sch)
      (update sch :default #(csk/->SCREAMING_SNAKE_CASE (name %)))
      sch)))

(defmethod edn-schema->avro-schema :fixed
  [edn-schema]
  (-> (fix-name edn-schema)
      (fix-aliases)))

(defmethod edn-schema->avro-schema :array
  [edn-schema]
  (update edn-schema :items edn-schema->avro-schema))

(defmethod edn-schema->avro-schema :map
  [edn-schema]
  (update edn-schema :values edn-schema->avro-schema))

(defmethod edn-schema->avro-schema :union
  [edn-schema]
  (mapv edn-schema->avro-schema edn-schema))

(defmethod edn-schema->avro-schema :name-keyword
  [kw]
  (edn-name-kw->avro-name kw))

(defmethod edn-schema->avro-schema :default
  [edn-schema]
  edn-schema)

(defmethod edn-schema->avro-schema :logical-type
  [edn-schema]
  (let [{:keys [logical-type]} edn-schema
        avro-schema (edn-schema->avro-schema (strip-lt-attrs edn-schema))]
    (assoc avro-schema :logicalType logical-type)))

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

(defmethod make-default-data-size :union
  [edn-schema name->edn-schema]
  (apply clojure.core/max (map #(make-default-data-size % name->edn-schema)
                               edn-schema)))

(defmethod make-default-data-size :name-keyword
  [name-kw name->edn-schema]
  100) ;; Possibly recursive schema, so just return a reasonable default

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
  (let [schema-name-str (:name schema)
        schema* (cond-> schema
                  (:namespace schema) (update :namespace csk/->kebab-case))]
    (if-not (fullname? schema-name-str)
      (assoc schema* :name (csk/->kebab-case-keyword schema-name-str))
      (let [schema-ns (-> (fullname->ns schema-name-str)
                          (java-namespace->clj-namespace))
            schema-name (-> (fullname->name schema-name-str)
                            (csk/->kebab-case))]
        (assoc schema* :name (keyword schema-ns schema-name))))))

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
  (let [m (avro-name->edn-name avro-schema)
        symbols (mapv csk/->kebab-case-keyword (:symbols m))]
    (-> m
        (update :type keyword)
        (assoc :symbols symbols)
        (assoc :default (first symbols)))))

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

(defn records-match?
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema]
  (or (= writer-edn-schema reader-edn-schema)
      (let [{writer-name :name
             writer-fields :fields} writer-edn-schema
            {reader-name :name
             reader-fields :fields} reader-edn-schema]
        (and (= writer-name reader-name)
             (reduce (fn [acc field-name]
                       (let [w-field (some #(when (= field-name (:name %)) %)
                                           writer-fields)
                             r-field (some #(when (= field-name (:name %)) %)
                                           reader-fields)]
                         (if (edn-schemas-match? (:type w-field) (:type r-field)
                                                 writer-name->edn-schema
                                                 reader-name->edn-schema)
                           acc
                           (reduced false))))
                     true
                     (set/union (set (map :name writer-fields))
                                (set (map :name reader-fields))))))))

(defn union-writer-match?
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema]
  ;; At least one of the writer's members must match the reader
  (reduce (fn [acc writer-item-schema]
            (if (edn-schemas-match? writer-item-schema reader-edn-schema
                                    writer-name->edn-schema
                                    reader-name->edn-schema)
              (reduced true)
              acc))
          false writer-edn-schema))

(defn union-reader-match?
  [writer-edn-schema reader-edn-schema
   writer-name->edn-schema reader-name->edn-schema]
  ;; At least one of the reader's members must match the writer
  (reduce (fn [acc reader-item-schema]
            (if (edn-schemas-match? writer-edn-schema reader-item-schema
                                    writer-name->edn-schema
                                    reader-name->edn-schema)
              (reduced true)
              acc))
          false reader-edn-schema))

(defn edn-schemas-match? [writer-edn-schema reader-edn-schema
                          writer-name->edn-schema reader-name->edn-schema]
  (when (nil? writer-edn-schema)
    (throw (ex-info "writer-edn-schema is nil."
                    (sym-map writer-edn-schema reader-edn-schema))))
  (when (nil? reader-edn-schema)
    (throw (ex-info "reader-edn-schema is nil."
                    (sym-map writer-edn-schema reader-edn-schema))))
  (let [writer-type (get-avro-type writer-edn-schema)
        reader-type (get-avro-type reader-edn-schema)]
    (cond
      (= :name-keyword writer-type)
      (edn-schemas-match? (writer-name->edn-schema writer-edn-schema)
                          reader-edn-schema
                          writer-name->edn-schema
                          reader-name->edn-schema)

      (= :name-keyword reader-type)
      (edn-schemas-match? writer-edn-schema
                          (reader-name->edn-schema reader-edn-schema)
                          writer-name->edn-schema
                          reader-name->edn-schema)

      :else
      (or
       (and (= :array writer-type) (= :array reader-type)
            (edn-schemas-match? (:items writer-edn-schema)
                                (:items reader-edn-schema)
                                writer-name->edn-schema
                                reader-name->edn-schema))

       (and (= :map writer-type) (= :map reader-type)
            (edn-schemas-match? (:values writer-edn-schema)
                                (:values reader-edn-schema)
                                writer-name->edn-schema
                                reader-name->edn-schema))

       (and (= :enum writer-type) (= :enum reader-type)
            (= (:name writer-edn-schema)
               (:name reader-edn-schema)))

       (and (= :fixed writer-type) (= :fixed reader-type)
            (= (:name writer-edn-schema)
               (:name reader-edn-schema))
            (= (:size writer-edn-schema)
               (:size reader-edn-schema)))

       (and (= :record writer-type) (= :record reader-type)
            (records-match? writer-edn-schema reader-edn-schema
                            writer-name->edn-schema
                            reader-name->edn-schema))

       (and (avro-primitive-types writer-type)
            (= writer-type reader-type))

       (and (= :int writer-type) (#{:long :float :double} reader-type))

       (and (= :long writer-type) (#{:float :double} reader-type))

       (and (= :float writer-type) (= :double reader-type))

       (and (= :string writer-type) (= :bytes reader-type))

       (and (= :bytes writer-type) (= :string reader-type))

       (and (= :union writer-type)
            (union-writer-match? writer-edn-schema reader-edn-schema
                                 writer-name->edn-schema
                                 reader-name->edn-schema))

       (and (= :union reader-type)
            (union-reader-match? writer-edn-schema reader-edn-schema
                                 writer-name->edn-schema
                                 reader-name->edn-schema))))))

(defn dedupe-schemas [schemas]
  (vals (reduce (fn [acc schema]
                  (let [fp (-> (fingerprint256 schema)
                               (ba/byte-array->b64))]
                    (assoc acc fp schema)))
                {} schemas)))
