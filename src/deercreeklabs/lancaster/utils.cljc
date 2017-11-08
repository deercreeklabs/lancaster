(ns deercreeklabs.lancaster.utils
  (:refer-clojure :exclude [long namespace-munge])
  (:require
   [camel-snake-kebab.core :as csk]
   #?(:clj [cheshire.core :as json])
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [puget.printer :refer [cprint]])
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster.utils)
     :clj
     (:import
      (com.google.common.primitives UnsignedLong))))

#?(:cljs
   (set! *warn-on-infer* true))

#?(:cljs (def class type))
#?(:cljs (def Long js/Long))

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defprotocol IOutputStream
  (write-long-varint-zz [this l])
  (write-byte [this b])
  (write-bytes [this bs num-bytes])
  (write-bytes-w-len-prefix [this bs])
  (write-utf8-string [this s])
  (write-float [this f])
  (write-double [this d])
  (to-byte-array [this]))

(defprotocol IInputStream
  (read-long-varint-zz [this])
  (read-byte [this])
  (read-bytes [this num-bytes])
  (read-len-prefixed-bytes [this])
  (read-utf8-string [this])
  (read-float [this])
  (read-double [this]))

(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-named-types #{:record :fixed :enum})
(def avro-complex-types #{:record :fixed :enum :array :map :union})

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
    (keyword (name (:namespace edn-schema)) (name (:name edn-schema)))

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

;; TODO: Fix this in cljs to handle a is number and b is a long
(s/defn long= :- s/Bool
  [a :- s/Any
   b :- s/Any]
  #?(:clj (= a b)
     :cljs (.equals a b)))

(s/defn long :- Long
  [x :- s/Any]
  (when-not (nil? x)
    #?(:clj (clojure.core/long x)
       :cljs (Long.fromValue x))))

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

(s/defn hex-str->long :- Long
  [hex-str :- s/Str]
  #?(:clj
     (.longValue (UnsignedLong/valueOf hex-str 16))
     :cljs
     (.fromString Long hex-str 16)))

(s/defn long->hex-str :- s/Str
  [l :- Long]
  (let [pad (fn [s len]
              (loop [out s]
                (if (= len (count out))
                  out
                  (recur (str "0" out)))))
        s #?(:clj
             (Long/toHexString l)
             :cljs
             (-> (.toUnsigned l)
                 (.toString 16)))]
    (pad s 16)))

(defn- throw-long->int-err [l]
  (throw (ex-info (str "Cannot convert long `" l "` to int.")
                  {:input l
                   :class-of-input (class l)})))

(s/defn long->int :- s/Int
  [l :- Long]
  #?(:clj (if (and (<= l 2147483647) (>= l -2147483648))
            (.intValue l)
            (throw-long->int-err l))
     :cljs (if (and (.lte l 2147483647) (.gte l -2147483648))
             (.toInt l)
             (throw-long->int-err l))))

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

(defn valid-int? [data]
  (and (integer? data)
       (<= data 2147483647)
       (>= data -2147483648)))

(defn valid-long? [data]
  (and (long-or-int? data)
       (<= data 9223372036854775807)
       (>= data -9223372036854775808)))

(defn valid-float? [data]
  (and (number? data)
       (<= data 3.4028234E38)
       (>= data -3.4028234E38)))

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

(defn more-than-one? [set schemas]
  (> (count (keep #(set (get-avro-type %)) schemas)) 1))

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

(defmethod make-serializer :null
  [edn-schema]
  (fn serialize [os data]
    (when-not (nil? data)
      (throw (ex-info "Data is not a valid :null. Must be nil."
                      (sym-map data))))))

(defmethod make-deserializer :null
  [edn-schema]
  (fn deserialize [is]
    nil))

(defmethod make-serializer :boolean
  [edn-schema]
  (fn serialize [os data]
    (when-not (boolean? data)
      (throw (ex-info "Data is not a valid :boolean." (sym-map data))))
    (write-byte os (if data 1 0))))

(defmethod make-deserializer :boolean
  [edn-schema]
  (fn deserialize [is]
    (= 1 (read-byte is))))

(defmethod make-serializer :int
  [edn-schema]
  (fn serialize [os data]
    (when-not (valid-int? data)
      (throw (ex-info "Data is not a valid :int." (sym-map data))))
    (write-long-varint-zz os data)))

(defmethod make-deserializer :int
  [edn-schema]
  (fn deserialize [is]
    (int (read-long-varint-zz is))))

(defmethod make-serializer :long
  [edn-schema]
  (fn serialize [os data]
    (when-not (valid-long? data)
      (throw (ex-info "Data is not a valid :long." (sym-map data))))
    (write-long-varint-zz os data)))

(defmethod make-deserializer :long
  [edn-schema]
  (fn deserialize [is]
    (read-long-varint-zz is)))

(defmethod make-serializer :float
  [edn-schema]
  (fn serialize [os data]
    (when-not (valid-float? data)
      (throw (ex-info "Data is not a valid :float." (sym-map data))))
    (write-float os data)))

(defmethod make-deserializer :float
  [edn-schema]
  (fn deserialize [is]
    (read-float is)))

(defmethod make-serializer :double
  [edn-schema]
  (fn serialize [os data]
    (when-not (valid-double? data)
      (throw (ex-info "Data is not a valid :double." (sym-map data))))
    (write-double os data)))

(defmethod make-deserializer :double
  [edn-schema]
  (fn deserialize [is]
    (read-double is)))

(defmethod make-serializer :string
  [edn-schema]
  (fn serialize [os data]
    (when-not (string? data)
      (throw (ex-info "Data is not a valid :string." (sym-map data))))
    (write-utf8-string os data)))

(defmethod make-deserializer :string
  [edn-schema]
  (fn deserialize [is]
    (read-utf8-string is)))

(defmethod make-serializer :bytes
  [edn-schema]
  (fn serialize [os data]
    (when-not (ba/byte-array? data)
      (throw (ex-info "Data is not a valid :bytes." (sym-map data))))
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
    (fn serialize [os data]
      (if-let [i (symbol->index data)]
        (write-long-varint-zz os i)
        (throw (ex-info "Data is not a one of the valid symbols."
                        (sym-map symbols data)))))))

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
    (fn serialize [os data]
      (when-not (ba/byte-array? data)
        (throw (ex-info "Data is not a valid :fixed." (sym-map data))))
      (when-not (= size (count data))
        (throw (ex-info "Data is not the proper size."
                        {:data data
                         :data-size (count data)
                         :schema-size size})))
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
    (fn serialize [os data]
      (when-not (map? data)
        (throw (ex-info "Data is not a valid :map." (sym-map data))))
      (write-long-varint-zz os (count data))
      (doseq [[k v] data]
        (write-utf8-string os k)
        (serialize-value os v))
      (write-byte os 0))))

(defmethod make-deserializer :map
  [edn-schema]
  (let [{:keys [values]} edn-schema
        deserialize-value (make-deserializer values)]
    (fn deserialize [is]
      (let [m (transient {})]
        (loop []
          (let [count (long->int (read-long-varint-zz is))]
            (if (zero? count)
              (persistent! m)
              (do
                (dotimes [i count]
                  (let [k (read-utf8-string is)
                        v (deserialize-value is)]
                    (assoc! m k v)))
                (recur)))))))))

(defmethod make-serializer :array
  [edn-schema]
  (let [{:keys [items]} edn-schema
        serialize-item (make-serializer items)]
    (fn serialize [os data]
      (when-not (sequential? data)
        (throw (ex-info "Data is not a valid :array." (sym-map data))))
      (write-long-varint-zz os (count data))
      (doseq [item data]
        (serialize-item os item))
      (write-byte os 0))))

(defmethod make-deserializer :array
  [edn-schema]
  (let [{:keys [items]} edn-schema
        deserialize-item (make-deserializer items)]
    (fn deserialize [is]
      (let [a (transient [])]
        (loop []
          (let [count (long->int (read-long-varint-zz is))]
            (if (zero? count)
              (persistent! a)
              (do
                (dotimes [i count]
                  (conj! a (deserialize-item is)))
                (recur)))))))))

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
    (fn [data]
      (loop [i 0]
        (cond
          ((tests i) data) i
          (< i num-schemas) (recur (inc i))
          :else (throw (ex-info "Data does not match union schema."
                                (sym-map data))))))))

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
      (fn serialize [os data]
        (let [[schema-name data] data
              [branch serializer] (schema-name->branch-info schema-name)]
          (write-long-varint-zz os branch)
          (serializer os data))))
    (let [data->branch (make-data->branch edn-schema)
          branch->serializer (mapv make-serializer edn-schema)]
      (fn serialize [os data]
        (let [branch (data->branch data)
              serializer (branch->serializer branch)]
          (write-long-varint-zz os branch)
          (serializer os data))))))

(defmethod make-deserializer :union
  [edn-schema]
  (if (wrapping-required? edn-schema)
    (let [branch->deserializer (mapv make-deserializer edn-schema)
          branch->schema-name (mapv get-schema-name edn-schema)]
      (fn deserialize [is]
        (let [branch (read-long-varint-zz is)
              deserializer (branch->deserializer branch)
              schema-name (branch->schema-name branch)]
          [schema-name (deserializer is)])))
    (let [branch->deserializer (mapv make-deserializer edn-schema)
          branch->schema-name (mapv get-schema-name edn-schema)]
      (fn deserialize [is]
        (let [branch (read-long-varint-zz is)
              deserializer (branch->deserializer branch)]
          (deserializer is))))))

(defn make-field-info [field-schema]
  (let [{:keys [name type default]} field-schema
        serializer (make-serializer type)]
    [name default serializer]))

(defmethod make-serializer :record
  [edn-schema]
  (let [field-infos (mapv make-field-info (:fields edn-schema))]
    (fn serialize [os data]
      (doseq [[k default serializer] field-infos]
        (serializer os (get data k default))))))

(defmethod make-deserializer :record
  [edn-schema]
  (let [field-infos (mapv (fn [field]
                            (let [{:keys [name type]} field
                                  deserializer (make-deserializer type)]
                              [name deserializer]))
                          (:fields edn-schema))]
    (fn deserialize [is]
      (let [m (transient {})]
        (doseq [[k deserializer] field-infos]
          (assoc! m k (deserializer is)))
        (persistent! m)))))

(defn edn->json-string [edn]
  #?(:clj (json/generate-string edn {:pretty true})
     :cljs (js/JSON.stringify (clj->js edn))))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn}))

(s/defn get-current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

#_
(defn drop-schema-from-name [s]
  (if-not (clojure.string/ends-with? s "schema")
    s
    (-> (name s)
        (clojure.string/split #"-schema")
        (first))))
