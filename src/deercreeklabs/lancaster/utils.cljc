(ns deercreeklabs.lancaster.utils
  (:require
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster.utils)))

#?(:cljs
   (set! *warn-on-infer* true))

(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-named-types #{:record :fixed :enum})

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn short-log-output-fn [data]
  (let [{:keys [level msg_ ?ns-str ?file ?line]} data
        formatter (f/formatters  :hour-minute-second-ms)
        timestamp (f/unparse formatter (t/now))]
    (str
     timestamp " "
     (clojure.string/upper-case (name level))  " "
     "[" (or ?ns-str ?file "?") ":" (or ?line "?") "] - "
     @msg_)))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn short-log-output-fn}))

(defn get-current-time-ms
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn get-schema-name [schema]
  (cond
    (avro-primitive-types schema) schema
    (avro-named-types (:type schema)) (:name schema)
    (nil? schema) (throw (ex-info "Schema is nil."
                                  {:type :illegal-argument
                                   :subtype :schema-is-nil
                                   :schema schema}))
    :else schema))

(defn get-avro-type [schema]
  (cond
    (sequential? schema) :union
    (map? schema) (:type schema)
    (nil? schema) (throw (ex-info "Schema is nil."
                                  {:type :illegal-schema
                                   :subtype :schema-is-nil
                                   :schema schema}))
    :else schema))
