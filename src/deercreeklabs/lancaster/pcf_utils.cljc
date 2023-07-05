(ns deercreeklabs.lancaster.pcf-utils
  (:require
   [clojure.string :as string]
   [deercreeklabs.lancaster.impl :as impl]
   [deercreeklabs.lancaster.utils :as u]))

#?(:clj (set! *warn-on-reflection* true))

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
  (-> (select-keys sch [:type :name :namespace :fields])
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
  (let [{:keys [name type namespace]} field]
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
  (let [{:keys [name namespace fields]} sch
        name (cond (re-find #"\." name) name
                   namespace (str (u/clj-namespace->java-namespace namespace) "." name)
                   u/**enclosing-namespace** (str u/**enclosing-namespace** "." name)
                   :else name)
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
