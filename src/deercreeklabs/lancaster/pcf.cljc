(ns deercreeklabs.lancaster.pcf
  (:require
   [camel-snake-kebab.core :as csk]
   #?(:clj [cheshire.core :as json])
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defmulti filter-attrs u/avro-type-dispatch)
(defmulti xf-names u/avro-type-dispatch)
(defmulti emit u/avro-type-dispatch)

(def ^:dynamic **enclosing-namespace** nil)

(defn xf-name [sch]
  (let [sch-ns (or (:namespace sch) **enclosing-namespace**)
        sch-name (:name sch)
        fullname (cond
                   (u/fullname? sch-name) sch-name
                   (and sch-ns sch-name) (str sch-ns "." sch-name)
                   :else sch-name)]
    (-> sch
        (dissoc :namespace)
        (assoc :name fullname))))

(defmethod xf-names :enum
  [sch]
  (xf-name sch))

(defmethod xf-names :fixed
  [sch]
  (xf-name sch))

(defmethod xf-names :array
  [sch]
  (update sch :items xf-names))

(defmethod xf-names :map
  [sch]
  (update sch :values xf-names))

(defn xf-field-names [field]
  (update field :type xf-names))

(defmethod xf-names :record
  [sch]
  (let [new-sch (xf-name sch)
        new-ns (u/fullname->ns (:name new-sch))]
    (binding [**enclosing-namespace** new-ns]
      (update new-sch :fields #(mapv xf-field-names %)))))

(defmethod xf-names :union
  [sch]
  (map xf-names sch))

(defmethod xf-names :default
  [sch]
  sch)

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
        name-pair (str "\"name\":\"" name "\"")
        type-pair (str "\"type\":\"record\"")
        fields-pair (str "\"fields\":[" (emit-fields fields) "]")
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

(defmethod emit :string-reference
  [sch]
  (str "\"" sch "\""))

(defmethod emit :default
  [sch]
  (if (keyword? sch)
    (str "\"" (u/name-kw->name-str sch) "\"")
    sch))

(defn avro-schema->pcf [avro-schema]
  (if (string? avro-schema) ;; primitives are strings at this point
    (str "\"" avro-schema "\"")
    (-> avro-schema
        (xf-names)
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
      :string-reference)

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
                          (u/java-namespace->clj-namespace)
                          (keyword))
            schema-name (-> (u/fullname->name schema-name-str)
                            (csk/->kebab-case-keyword))]
        (assoc schema :namespace schema-ns :name schema-name)))))

(defmulti avro-schema->edn-schema pcf-type-dispatch)

(defmethod avro-schema->edn-schema :primitive
  [avro-schema]
  (keyword avro-schema))

(defmethod avro-schema->edn-schema :string-reference
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

(defn expand-references
  ([edn-schema]
   (expand-references edn-schema (atom {})))
  ([edn-schema *name->schema]

   (let [expand* #(expand-references % *name->schema)
         update-names! (fn [edn-schema]
                         (let [schema-name (u/name-kw->name-str
                                            (u/get-schema-name edn-schema))]
                           (swap! *name->schema assoc schema-name edn-schema)
                           edn-schema))
         avro-type (u/get-avro-type edn-schema)]

     (case avro-type
       :enum (update-names! edn-schema)
       :fixed (update-names! edn-schema)
       :array (update edn-schema :items expand*)
       :map (update edn-schema :values expand*)
       :union (mapv expand* edn-schema)
       :record (let [edn-schema (update-names! edn-schema)
                     fix-field (fn [field]
                                 (update field :type expand*))]
                 (update edn-schema :fields #(mapv fix-field %)))
       :keyword-reference (@*name->schema (u/name-kw->name-str edn-schema))
       edn-schema))))

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
    (-> pcf
        (u/json-string->edn)
        (avro-schema->edn-schema)
        (expand-references))))
