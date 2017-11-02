(ns deercreeklabs.lancaster.utils
  (:refer-clojure :exclude [namespace-munge])
  (:require
   [camel-snake-kebab.core :as csk]
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [puget.printer :refer [cprint]])
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster.utils)))

#?(:cljs
   (set! *warn-on-infer* true))

(declare get-avro-type get-field-default)

(def WrappedData {s/Keyword s/Any})

(def avro-primitive-types #{:null :boolean :int :long :float :double
                            :bytes :string})
(def avro-named-types   #{:record :fixed :enum})
(def avro-complex-types #{:record :fixed :enum :array :map :union})

(defprotocol IAvroSchema
  (serialize [this data])
  (deserialize [this writer-json-schema ba return-native?])
  (wrap [this data])
  (get-edn-schema [this])
  (get-json-schema [this])
  (get-parsing-canonical-form [this])
  (get-fingerprint128 [this]))

;;;;;;;;;;;;;;;;;;;; Macro-writing utils ;;;;;;;;;;;;;;;;;;;;

;; From: (str "http://blog.nberger.com.ar/blog/2015/09/18/"
"more-portable-complex-macro-musing/"
(defn- cljs-env?
  "Take the &env from a macro, and return whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

(defmacro if-cljs
  "Return then if we are generating cljs code and else for Clojure code.
  https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
  [then else]
  (if (cljs-env? &env) then else))


;;;;;;;;;;;;;;;;;;;; Multimethods & Dispatch Fns ;;;;;;;;;;;;;;;;;;;;

(defn first-arg-dispatch [first-arg & rest-of-args]
  first-arg)

(defn avro-type-dispatch [edn-schema & args]
  (get-avro-type edn-schema))

(defmulti make-edn-schema first-arg-dispatch)
(defmulti edn-schema->avro-schema avro-type-dispatch)
(defmulti make-constructor avro-type-dispatch)
(defmulti make-post-converter avro-type-dispatch)
(defmulti clj->avro first-arg-dispatch)
(defmulti avro->clj first-arg-dispatch)

;;;;;;;;;;;;;;;;;;;; Utility Fns ;;;;;;;;;;;;;;;;;;;;

(defn ensure-edn-schema [schema]
  (cond
    (satisfies? IAvroSchema schema)
    (get-edn-schema schema)

    (string? schema)
    (if (clojure.string/includes? schema ".")
      schema
      (str *ns* "." schema))

    (sequential? schema)
    (mapv ensure-edn-schema schema) ;; union

    :else
    schema))

(defn make-default-record [record-edn-schema default-record]
  (reduce (fn [acc field]
            (let [{field-name :name
                   field-type :type
                   field-default :default} field
                  field-schema (ensure-edn-schema field-type)
                  v (get-field-default field-schema
                                       (field-name default-record))]
              (assoc acc field-name v)))
          {} (:fields record-edn-schema)))

(defn byte-array->byte-str [ba]
  (apply str (map char ba)))

(defn make-default-fixed-or-bytes [default]
  (byte-array->byte-str (or default (ba/byte-array []))))

(defn get-field-default [field-schema field-default]
  (let [avro-type (get-avro-type field-schema)]
    (case avro-type
      :record (make-default-record field-schema field-default)
      :union (get-field-default (first field-schema) field-default)
      :fixed (make-default-fixed-or-bytes field-default)
      :bytes (make-default-fixed-or-bytes field-default)
      (or field-default
          (case avro-type
            :null nil
            :boolean false
            :int (int -1)
            :long -1
            :float (float -1.0)
            :double (double -1.0)
            :string ""
            :enum (first (:symbols field-schema))
            :array []
            :map {})))))

(defmethod make-edn-schema :primitive
  [schema-type schema-ns short-name primitive-type]
  primitive-type)

(defn make-record-field [[field-name field-type field-default]]
  (let [field-schema (ensure-edn-schema field-type)]
    {:name (keyword field-name)
     :type field-schema
     :default (get-field-default field-schema
                                 field-default)}))

(defmethod make-edn-schema :record
  [schema-type schema-ns short-name args]
  (let [base {:namespace schema-ns
              :name (keyword short-name)
              :type :record}]
    (if (map? (first args))
      (-> (merge base (first args))
          (update :fields #(mapv make-record-field %)))
      (assoc base :fields (mapv make-record-field args)))))

(defmethod make-edn-schema :enum
  [schema-type schema-ns short-name symbols]
  {:namespace schema-ns
   :name (keyword short-name)
   :type :enum
   :symbols symbols})

(defmethod make-edn-schema :fixed
  [schema-type schema-ns short-name size]
  {:namespace schema-ns
   :name (keyword short-name)
   :type :fixed
   :size size})

(defmethod make-edn-schema :array
  [schema-type schema-ns short-name items]
  {:type :array
   :items (ensure-edn-schema items)})

(defmethod make-edn-schema :map
  [schema-type schema-ns short-name values]
  {:type :map
   :values (ensure-edn-schema values)})

(defmethod make-edn-schema :union
  [schema-type _ _ member-schemas]
  (mapv ensure-edn-schema member-schemas))

(defn drop-schema-from-name [s]
  (if-not (clojure.string/ends-with? s "schema")
    s
    (-> (name s)
        (clojure.string/split #"-schema")
        (first))))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn}))

(defn get-schema-name [edn-schema]
  (cond
    (avro-named-types (:type edn-schema))
    (if-let [schema-ns (:namespace edn-schema)]
      (str schema-ns "." (name (:name edn-schema)))
      (name (:name edn-schema)))

    (map? edn-schema)
    (:type edn-schema)

    (nil? edn-schema)
    (throw (ex-info "Schema is nil."
                    {:type :illegal-argument
                     :subtype :schema-is-nil
                     :schema edn-schema}))

    :else
    edn-schema))

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

(defn edn-schema->dispatch-name [edn-schema]
  (let [avro-type (get-avro-type edn-schema)]
    (cond
      (or (avro-primitive-types avro-type)
          (avro-named-types avro-type))
      (get-schema-name edn-schema)

      (= :union avro-type)
      (str "union-of-" (clojure.string/join
                        "," (map #(name (edn-schema->dispatch-name %))
                                 edn-schema)))

      (= :map avro-type)
      (str "map-of-" (name (edn-schema->dispatch-name
                            (:values edn-schema))))

      (= :array avro-type)
      (str "array-of-" (name (edn-schema->dispatch-name
                              (:items edn-schema))))
      (string? edn-schema)
      edn-schema

      :else
      (throw (ex-info (str "Could not find dispatch name for " edn-schema)
                      {:edn-schema edn-schema})))))

(defn more-than-one? [set schemas]
  (> (count (keep #(set (get-avro-type %)) schemas)) 1))

(defn wrapping-required? [schemas]
  (or (more-than-one? #{:map :record} schemas)
      (more-than-one? #{:int :long :float :double} schemas)
      (more-than-one? #{:null} schemas)
      (more-than-one? #{:boolean} schemas)
      (more-than-one? #{:string :enum} schemas)
      (more-than-one? #{:bytes :fixed} schemas)
      (more-than-one? #{:array} schemas)))

(defn namespace-munge [ns]
  (clojure.string/replace (str ns) #"-" "_"))

(defn edn-name->avro-name [s]
  (if (clojure.string/includes? s ".")
    (let [name-parts (clojure.string/split s #"\.")
          schema-ns (clojure.string/join "." (butlast name-parts))
          schema-name (last name-parts)]
      (str (namespace-munge schema-ns) "." (csk/->PascalCase schema-name)))
    (csk/->PascalCase s)))
