(ns deercreeklabs.lancaster.utils
  (:require
   [camel-snake-kebab.core :as csk]
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   #?(:clj [puget.printer :refer [cprint]])
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

;; We have to name this pprint* to not conflict with
;; clojure.repl/pprint, which gets loaded into the repl's namespace
(defn pprint*
  "Pretty-prints its argument. Color is used in clj, but not cljs."
  [x]
  (#?(:clj cprint :cljs cljs.pprint/pprint) x)
  nil)

(defn pprint-str
  "Like pprint, but returns a string."
  [x]
  (with-out-str
    (pprint* x)))

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

(defn drop-schema-from-name [s]
  (-> (name s)
      (clojure.string/split #"-schema")
      (first)))

(defn make-default-record [schema]
  (let [add-field (fn [acc {:keys [type name default]}]
                    (let [avro-type (get-avro-type type)
                          val (if (= :record avro-type)
                                (make-default-record type)
                                default)]
                      (assoc acc name val)))]
    (reduce add-field {} (:fields schema))))

(defn make-default-enum [enum-schema field-default]
  (let [sym (or field-default
                (first (:symbols enum-schema)))]
    (-> (name sym)
        (csk/->SCREAMING_SNAKE_CASE))))

(defn get-field-default [field-schema field-default]
  (let [avro-type (get-avro-type field-schema)]
    (if (= :enum avro-type)
      (make-default-enum field-schema field-default)
      (or field-default
          (case avro-type
            :null nil
            :boolean false
            :int (int -1)
            :long -1
            :float (float -1.0)
            :double (double -1.0)
            :bytes ""
            :string ""
            :array []
            :map {}
            :fixed ""
            :union (first field-schema)
            :record (make-default-record field-schema))))))

(defn make-named-schema*
  [schema-ns schema-name]
  (let [avro-name (csk/->PascalCase (name schema-name))
        schema (vary-meta
                {:namespace nil ;; declare this now to preserve key order
                 :name avro-name}
                assoc :avro-schema true)]
    (if schema-ns
      (assoc schema :namespace (namespace-munge (name schema-ns)))
      (dissoc schema :namespace))))

(defmacro named-schema-helper*
  [schema-fn schema-name args]
  (let [name* (drop-schema-from-name schema-name)
        args* (if (sequential? args)
                (vec args)
                args)]
    `(def ~(vary-meta schema-name assoc :avro-schema true)
       (let [ns# (.getName *ns*)]
         (~schema-fn ns# ~name* ~args*)))))
