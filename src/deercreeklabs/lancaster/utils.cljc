(ns deercreeklabs.lancaster.utils
  (:refer-clojure :exclude [long])
  (:require
   [camel-snake-kebab.core :as csk]
   #?(:clj [cheshire.core :as json])
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:cljs [goog.math :as gm])
   #?(:clj [primitive-math :as pm])
   #?(:clj [puget.printer :refer [cprint]])
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [deercreeklabs.lancaster.utils :refer [sym-map]])))

#?(:cljs
   (set! *warn-on-infer* true))

#?(:cljs (def class type))
#?(:cljs (def Long gm/Long))

#?(:cljs (def max-int (.fromInt Long 2147483647)))
#?(:cljs (def min-int (.fromInt Long -2147483648)))

#?(:clj (pm/use-primitive-operators))

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defprotocol IAvroSchema
  (serialize [this os data])
  (deserialize [this writer-pcf is])
  (wrap [this data])
  (get-edn-schema [this])
  (get-json-schema [this])
  (get-parsing-canonical-form [this])
  (get-fingerprint64 [this]))

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

(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-named-types #{:record :fixed :enum})
(def avro-complex-types #{:record :fixed :enum :array :map :union})

(defn make-schema-name [clj-name]
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

(defn clj-namespace->java-namespace [ns]
  (clojure.string/replace (str ns) #"-" "_"))

(defn java-namespace->clj-namespace [ns]
  (clojure.string/replace (str ns) #"_" "-"))

(defn edn-name->avro-name [s]
  (if (fullname? s)
    (let [name-parts (clojure.string/split s #"\.")
          schema-ns (clojure.string/join "." (butlast name-parts))
          schema-name (last name-parts)]
      (str (clj-namespace->java-namespace schema-ns) "."
           (csk/->PascalCase schema-name)))
    (csk/->PascalCase s)))

(defn get-avro-type [edn-schema]
  (cond
    (sequential? edn-schema) :union
    (map? edn-schema) (:type edn-schema)
    (nil? edn-schema) (throw (ex-info "Schema is nil."
                                      {:type :illegal-schema
                                       :subtype :schema-is-nil
                                       :schema edn-schema}))
    (string? edn-schema) :string-reference
    :else edn-schema))

(defn get-schema-name [edn-schema]
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

(defn byte-array->byte-str [ba]
  (apply str (map char ba)))

(defn first-arg-dispatch [first-arg & rest-of-args]
  first-arg)

(defn avro-type-dispatch [edn-schema & args]
  (get-avro-type edn-schema))

(defmulti make-serializer avro-type-dispatch)
(defmulti make-deserializer avro-type-dispatch)

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
       (<= (int data) 2147483647)
       (>= (int data) -2147483648)))

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
             (long? a) (.equals a b)
             (long? b) (.equals b a)
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
  #?(:clj (if (and (<= l 2147483647) (>= l -2147483648))
            (.intValue l)
            (throw-long->int-err l))
     :cljs (if-not (long? l)
             l
             (if (and (.lessThanOrEqual l 2147483647)
                      (.greaterThanOrEqual l -2147483648))
               (.toInt l)
               (throw-long->int-err l)))))

(defn int->long [int]
  #?(:clj (clojure.core/long int)
     :cljs (.fromInt Long int)))

(defn more-than-one? [schema-set schemas]
  (> (count (keep #(schema-set (get-avro-type %)) schemas)) 1))

(defn contains-union? [schemas]
  (some #(= :union (get-avro-type %)) schemas))

(defn illegal-union? [schemas]
  (or (contains-union? schemas)
      (more-than-one? #{:int} schemas)
      (more-than-one? #{:long} schemas)
      (more-than-one? #{:float} schemas)
      (more-than-one? #{:double} schemas)
      (more-than-one? #{:null} schemas)
      (more-than-one? #{:boolean} schemas)
      (more-than-one? #{:string} schemas)
      (more-than-one? #{:bytes} schemas)
      (more-than-one? #{:map} schemas)
      (more-than-one? #{:array} schemas)))

(defn wrapping-required? [schemas]
  (or (more-than-one? #{:map :record} schemas)
      (more-than-one? #{:int :long :float :double} schemas)
      (more-than-one? #{:bytes :fixed} schemas)))

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
            out (.getZero Long)]
       (let [b (.fromNumber Long (read-byte input-stream))]
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

(defmethod make-serializer :null
  [edn-schema]
  (fn serialize [os data path]
    (when-not (nil? data)
      (throw-invalid-data-error edn-schema data path))))

(defmethod make-deserializer :null
  [edn-schema]
  (fn deserialize [is]
    nil))

(defmethod make-serializer :boolean
  [edn-schema]
  (fn serialize [os data path]
    (when-not (boolean? data)
      (throw
       (throw-invalid-data-error edn-schema data path)))
    (write-byte os (if data 1 0))))

(defmethod make-deserializer :boolean
  [edn-schema]
  (fn deserialize [is]
    (= 1 (read-byte is))))

(defmethod make-serializer :int
  [edn-schema]
  (fn serialize [os data path]
    (when-not (valid-int? data)
      (throw-invalid-data-error edn-schema data path))
    (write-long-varint-zz os data)))

(defmethod make-deserializer :int
  [edn-schema]
  (fn deserialize [is]
    (int (read-long-varint-zz is))))

(defmethod make-serializer :long
  [edn-schema]
  (fn serialize [os data path]
    (when-not (valid-long? data)
      (throw-invalid-data-error edn-schema data path))
    (write-long-varint-zz os data)))

(defmethod make-deserializer :long
  [edn-schema]
  (fn deserialize [is]
    (read-long-varint-zz is)))

(defmethod make-serializer :float
  [edn-schema]
  (fn serialize [os data path]
    (when-not (valid-float? data)
      (throw-invalid-data-error edn-schema data path))
    (write-float os data)))

(defmethod make-deserializer :float
  [edn-schema]
  (fn deserialize [is]
    (read-float is)))

(defmethod make-serializer :double
  [edn-schema]
  (fn serialize [os data path]
    (when-not (valid-double? data)
      (throw-invalid-data-error edn-schema data path))
    (write-double os data)))

(defmethod make-deserializer :double
  [edn-schema]
  (fn deserialize [is]
    (read-double is)))

(defmethod make-serializer :string
  [edn-schema]
  (fn serialize [os data path]
    (when-not (string? data)
      (throw-invalid-data-error edn-schema data path))
    (write-utf8-string os data)))

(defmethod make-deserializer :string
  [edn-schema]
  (fn deserialize [is]
    (read-utf8-string is)))

(defmethod make-serializer :bytes
  [edn-schema]
  (fn serialize [os data path]
    (when-not (ba/byte-array? data)
      (throw-invalid-data-error edn-schema data path))
    (write-bytes-w-len-prefix os data)))

(defmethod make-deserializer :bytes
  [edn-schema]
  (fn deserialize [is]
    (read-len-prefixed-bytes is)))

(defmethod make-serializer :enum
  [edn-schema]
  (let [{:keys [symbols]} edn-schema
        symbol->index (apply hash-map
                             (apply concat (map-indexed
                                            #(vector %2 %1) symbols)))]
    (fn serialize [os data path]
      (if-let [i (symbol->index data)]
        (write-long-varint-zz os i)
        (throw
         (let [display-data (if (nil? data)
                              "nil"
                              data)]
           (ex-info (str "Data (" display-data
                         ") is not one of the symbols of this enum. Path: "
                         path)
                    (sym-map data path symbols edn-schema))))))))

(defmethod make-deserializer :enum
  [edn-schema]
  (let [{:keys [symbols]} edn-schema
        index->symbol (apply hash-map
                             (apply concat (map-indexed vector symbols)))]
    (fn deserialize [is]
      (index->symbol (read-long-varint-zz is)))))

(defmethod make-serializer :fixed
  [edn-schema]
  (let [{:keys [size]} edn-schema]
    (fn serialize [os data path]
      (when-not (ba/byte-array? data)
        (throw-invalid-data-error edn-schema data path))
      (when-not (= size (count data))
        (throw
         (ex-info (str "Data (" (ba/byte-array->debug-str data)
                       ") is not the proper size (" size ". Path: " path)
                  {:data data
                   :data-size (count data)
                   :schema-size size
                   :path path})))
      (write-bytes os data size))))

(defmethod make-deserializer :fixed
  [edn-schema]
  (let [{:keys [size]} edn-schema]
    (fn deserialize [is]
      (read-bytes is size))))

(defmethod make-serializer :map
  [edn-schema]
  (let [{:keys [values]} edn-schema
        serialize-value (make-serializer values)]
    (fn serialize [os data path]
      (when-not (map? data)
        (throw-invalid-data-error edn-schema data path))
      (write-long-varint-zz os (count data))
      (doseq [[k v] data]
        (write-utf8-string os k)
        (serialize-value os v (conj path k)))
      (write-byte os 0))))

(defmethod make-deserializer :map
  [edn-schema]
  (let [{:keys [values]} edn-schema
        deserialize-value (make-deserializer values)]
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

(defmethod make-serializer :array
  [edn-schema]
  (let [{:keys [items]} edn-schema
        serialize-item (make-serializer items)]
    (fn serialize [os data path]
      (when-not (sequential? data)
        (throw-invalid-data-error edn-schema data path))
      (write-long-varint-zz os (count data))
      (doall
       (map-indexed (fn [i item]
                      (serialize-item os item (conj path i)))
                    data))
      (write-byte os 0))))

(defmethod make-deserializer :array
  [edn-schema]
  (let [{:keys [items]} edn-schema
        deserialize-item (make-deserializer items)]
    (fn deserialize [is]
      (loop [a (transient [])]
        (let [count (int (long->int (read-long-varint-zz is)))]
          (if (zero? count)
            (persistent! a)
            (recur (reduce (fn [acc i]
                             (conj! acc (deserialize-item is)))
                           a (range count)))))))))

(defn get-test [edn-schema]
  (case (get-avro-type edn-schema)
    :null nil?
    :boolean boolean?
    :int number?
    :long number?
    :float number?
    :double number?
    :bytes ba/byte-array?
    :string string?
    :record map?
    :enum keyword?
    :array sequential?
    :map map?
    :fixed ba/byte-array?))

(defn make-data->branch [member-schemas]
  (let [num-schemas (count member-schemas)
        tests (mapv get-test member-schemas)]
    (fn [data path]
      (loop [i 0]
        (cond
          ((tests i) data) i
          (< i num-schemas) (recur (inc i))
          :else (throw
                 (ex-info (str "Data (" data ") does not match union schema."
                               " Path: " path)
                          (sym-map data path member-schemas))))))))

(defn make-schema-name->branch-info [edn-schema]
  (reduce (fn [acc i]
            (let [sch (nth edn-schema i)
                  serializer (make-serializer sch)]
              (assoc acc (get-schema-name sch) [i serializer])))
          {} (range (count edn-schema))))

(defmethod make-serializer :union
  [edn-schema]
  (if (wrapping-required? edn-schema)
    (let [schema-name->branch-info (make-schema-name->branch-info edn-schema)]
      (fn serialize [os data path]
        (let [[schema-name data] data
              [branch serializer] (schema-name->branch-info schema-name)]
          (when-not branch
            (let [schema-names (keys schema-name->branch-info)]
              (throw (ex-info (str "Schema name `" schema-name "` is not in "
                                   "the union schema.")
                              (sym-map schema-name schema-names data)))))
          (write-long-varint-zz os branch)
          (serializer os data path))))
    (let [data->branch (make-data->branch edn-schema)
          branch->serializer (mapv make-serializer edn-schema)]
      (fn serialize [os data path]
        (let [branch (data->branch data path)
              serializer (branch->serializer branch)]
          (write-long-varint-zz os branch)
          (serializer os data path))))))

(defmethod make-deserializer :union
  [edn-schema]
  (let [branch->deserializer (mapv make-deserializer edn-schema)
        branch->schema-name (mapv get-schema-name edn-schema)]
    (fn deserialize [is]
      (let [branch (read-long-varint-zz is)
            deserializer (branch->deserializer branch)
            data (deserializer is)]
        (if-not (wrapping-required? edn-schema)
          data
          (let [schema-name (branch->schema-name branch)]
            [schema-name data]))))))

(defn make-field-info [field-schema]
  (let [{:keys [name type default]} field-schema
        serializer (make-serializer type)]
    [name default serializer]))

(defmethod make-serializer :record
  [edn-schema]
  (let [field-infos (mapv make-field-info (:fields edn-schema))]
    (fn serialize [os data path]
      (when-not (map? data)
        (throw-invalid-data-error edn-schema data path))
      (doseq [[k default serializer] field-infos]
        (serializer os (get data k default) (conj path k))))))

(defmethod make-deserializer :record
  [edn-schema]
  (let [field-infos (mapv (fn [field]
                            (let [{:keys [name type]} field
                                  deserializer (make-deserializer type)]
                              [name deserializer]))
                          (:fields edn-schema))]
    (fn deserialize [is]
      (persistent!
       (reduce (fn [acc [k deserializer]]
                 (assoc! acc k (deserializer is)))
               (transient {}) field-infos)))))

(defn edn->json-string [edn]
  #?(:clj (json/generate-string edn {:pretty true})
     :cljs (js/JSON.stringify (clj->js edn))))

(defn json-string->edn [json-str]
  #?(:clj (json/parse-string json-str true)
     :cljs (js->clj (js/JSON.parse json-str) :keywordize-keys true)))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn}))

(s/defn get-current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))
