(ns deercreeklabs.lancaster.pcf
  (:require
   [camel-snake-kebab.core :as csk]
   #?(:clj [cheshire.core :as json])
   [clojure.string :as string]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defmulti filter-attrs u/avro-type-dispatch)
(defmulti emit u/avro-type-dispatch)

(defn fix-field-attrs [fields]
  (mapv #(-> (select-keys % [:type :name])
             (update :type filter-attrs))
        fields))

(defmethod filter-attrs :map
  [sch]
  (-> (select-keys sch [:type :values])
      (update :values filter-attrs)))

(defmethod filter-attrs :array
  [sch]
  (-> (select-keys sch [:type :items])
      (update :items filter-attrs)))

(defmethod filter-attrs :enum
  [sch]
  (select-keys sch [:type :name :symbols]))

(defmethod filter-attrs :fixed
  [sch]
  (select-keys sch [:type :name :size]))

(defmethod filter-attrs :record
  [sch]
  (-> (select-keys sch [:type :name :fields])
      (update :fields fix-field-attrs)))

(defmethod filter-attrs :union
  [sch]
  (mapv filter-attrs sch))

(defmethod filter-attrs :default
  [sch]
  (if (map? sch)
    (:type sch)
    sch))

(defn emit-field [field]
  (let [{:keys [name type]} field]
    (str "{\"name\":\"" name "\",\"type\":" (emit type) "}")))

(defn emit-fields [fields]
  (clojure.string/join "," (map emit-field fields)))

(defn emit-enum-symbol [symbol]
  (str "\"" (name symbol) "\""))

(defmethod emit :null
  [sch]
  "\"null\"")

(defmethod emit :boolean
  [sch]
  "\"boolean\"")

(defmethod emit :int
  [sch]
  "\"int\"")

(defmethod emit :long
  [sch]
  "\"long\"")

(defmethod emit :float
  [sch]
  "\"float\"")

(defmethod emit :double
  [sch]
  "\"double\"")

(defmethod emit :bytes
  [sch]
  "\"bytes\"")

(defmethod emit :string
  [sch]
  "\"string\"")

(defmethod emit :enum
  [sch]
  (str "{\"name\":\"" (:name sch) "\",\"type\":\"enum\",\"symbols\":["
       (clojure.string/join "," (map emit-enum-symbol (:symbols sch)))
       "]}"))

(defmethod emit :fixed
  [sch]
  (str "{\"name\":\"" (:name sch) "\",\"type\":\"fixed\",\"size\":"
       (:size sch) "}"))

(defmethod emit :record
  [sch]
  (let [{:keys [name fields]} sch
        sch-ns (u/fullname->ns name)
        name-pair (str "\"name\":\"" name "\"")
        type-pair (str "\"type\":\"record\"")
        fields-pair (binding [u/**enclosing-namespace** sch-ns]
                      (str "\"fields\":[" (emit-fields fields) "]"))
        attrs (clojure.string/join "," [name-pair type-pair fields-pair])]
    (str "{" attrs "}")))

(defmethod emit :array
  [sch]
  (str "{\"type\":\"array\",\"items\":" (emit (:items sch)) "}"))

(defmethod emit :map
  [sch]
  (str "{\"type\":\"map\",\"values\":" (emit (:values sch)) "}"))

(defmethod emit :union
  [sch]
  (let [schs (clojure.string/join "," (map emit sch))]
    (str "[" schs "]")))

(defmethod emit :name-keyword
  [sch]
  (str "\"" (u/name-kw->name-str sch) "\""))

(defmethod emit :name-string
  [name-str]
  (let [fullname (if (or (u/fullname? name-str)
                         (not u/**enclosing-namespace**))
                   name-str
                   (str u/**enclosing-namespace** "." name-str))]
    (str "\"" fullname "\"")))

(defn avro-schema->pcf [avro-schema]
  (if (string? avro-schema) ;; primitives are strings at this point
    (str "\"" avro-schema "\"")
    (-> avro-schema
        (filter-attrs)
        (emit))))

(defn pcf-type-dispatch [avro-schema]
  (cond
    (map? avro-schema)
    (let [{:keys [type]} avro-schema
          type-kw (keyword type)]
      (if (u/avro-primitive-types type-kw)
        :primitive
        type-kw))

    (string? avro-schema)
    (if (u/avro-primitive-type-strings avro-schema)
      :primitive
      :name-string)

    (sequential? avro-schema)
    :union))

(defn avro-name-str->edn-name-kw [name-str]
  (if (u/fullname? name-str)
    (let [ns (u/java-namespace->clj-namespace (u/fullname->ns name-str))
          name (csk/->kebab-case (u/fullname->name name-str))]
      (keyword ns name))
    (csk/->kebab-case-keyword name-str)))

(defn avro-name->edn-name [schema]
  (let [schema-name-str (:name schema)]
    (if-not (u/fullname? schema-name-str)
      (assoc schema :name (csk/->kebab-case-keyword schema-name-str))
      (let [schema-ns (-> (u/fullname->ns schema-name-str)
                          (u/java-namespace->clj-namespace))
            schema-name (-> (u/fullname->name schema-name-str)
                            (csk/->kebab-case))]
        (assoc schema :name (keyword schema-ns schema-name))))))

(defmulti avro-schema->edn-schema pcf-type-dispatch)

(defmethod avro-schema->edn-schema :primitive
  [avro-schema]
  (keyword avro-schema))

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

(defmulti add-defaults u/avro-type-dispatch)

(defmethod add-defaults :default
  [edn-schema name->edn-schema]
  edn-schema)

(defmethod add-defaults :array
  [edn-schema name->edn-schema]
  (update edn-schema :items #(add-defaults % name->edn-schema)))

(defmethod add-defaults :map
  [edn-schema name->edn-schema]
  (update edn-schema :values #(add-defaults % name->edn-schema)))

(defmethod add-defaults :union
  [edn-schema name->edn-schema]
  (mapv #(add-defaults % name->edn-schema) edn-schema))

(defmethod add-defaults :record
  [edn-schema name->edn-schema]
  (update edn-schema :fields
          (fn [fields]
            (mapv
             (fn [field]
               (let [{:keys [type]} field
                     default (u/get-default-data type nil name->edn-schema)
                     new-type (add-defaults type name->edn-schema)
                     field-type (u/get-avro-type type)
                     new-field (-> field
                                   (assoc :default default)
                                   (assoc :type new-type))]
                 new-field))
             fields))))

(defn pcf->edn-schema [pcf]
  (case pcf
    "null" :null
    "boolean" :boolean
    "int" :int
    "long" :long
    "float" :float
    "double" :double
    "bytes" :bytes
    "string" :string
    (let [edn-schema (avro-schema->edn-schema (u/json-string->edn pcf))
          name->edn-schema (u/make-name->edn-schema edn-schema)]
      (add-defaults edn-schema name->edn-schema))))
