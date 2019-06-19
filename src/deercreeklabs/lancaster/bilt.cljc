(ns deercreeklabs.lancaster.bilt
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [schema.core :as s]))

(defn flex-map->rec [m]
  {:ks (vec (keys m))
   :vs (vec (vals m))})

(defn rec->flex-map [r]
  (let [{:keys [ks vs]} r]
    (zipmap ks vs)))

(defn flex-map-schema
  [name-kw logical-type key-schema values-schema valid-k?]
  (schemas/validate-name-kw name-kw)
  (let [key-edn-schema (l/edn key-schema)
        values-edn-schema (l/edn values-schema)
        fields [{:name :ks
                 :type {:type :array
                        :items key-edn-schema}
                 :default []}
                {:name :vs
                 :type {:type :array
                        :items values-edn-schema}
                 :default []}]
        kn->es (u/make-name->edn-schema key-edn-schema)
        vn->es (u/make-name->edn-schema values-edn-schema)
        kp-schema (u/edn-schema->plumatic-schema key-edn-schema kn->es)
        vp-schema (u/edn-schema->plumatic-schema values-edn-schema vn->es)
        edn-schema {:name name-kw
                    :type :record
                    :key-ns-type :none
                    :fields fields
                    :logical-type logical-type
                    :lt->avro flex-map->rec
                    :avro->lt rec->flex-map
                    :lt? #(and (map? %) (valid-k? (ffirst %)))
                    :valid-k? valid-k?
                    :k->child-edn-schema (constantly values-edn-schema)
                    :edn-sub-schemas (set [key-edn-schema values-edn-schema])
                    :plumatic-schema {kp-schema vp-schema}}]
    (l/edn->schema edn-schema)))

;; TODO: Validate args

(defn int-map-schema
  "Creates a Lancaster schema object representing a map of `int` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw values-schema]
  (flex-map-schema name-kw "int-map" l/int-schema values-schema int?))

(defn long-map-schema
  "Creates a Lancaster schema object representing a map of `long` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw values-schema]
  (flex-map-schema name-kw "long-map" l/long-schema values-schema
                   u/long-or-int?))

(defn fixed-map-schema
  "Creates a Lancaster schema object representing a map of `long` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw key-size values-schema]
  (when-not (nat-int? key-size)
    (throw (ex-info (str "Second argument to fixed-map-schema must be a "
                         "positive integer. Got `" key-size "`.")
                    (u/sym-map key-size ))))
  (let [key-schema-name (keyword (namespace name-kw)
                                 (str (name name-kw) "-value"))
        key-schema (l/fixed-schema key-schema-name key-size)]
    (flex-map-schema name-kw "fixed-map" key-schema
                     values-schema ba/byte-array?)))

(defmacro def-int-map-schema
  "Defines a var whose value is an int-map-schema object"
  [clj-name values-schema]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (int-map-schema ~name-kw ~values-schema))))

(defmacro def-long-map-schema
  "Defines a var whose value is an long-map-schema object"
  [clj-name values-schema]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (long-map-schema ~name-kw ~values-schema))))

(defmacro def-fixed-map-schema
  "Defines a var whose value is an fixed-map-schema object"
  [clj-name key-size values-schema]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))             ;; clj
        schema-name (u/schema-name clj-name)
        name-kw (keyword ns-name schema-name)]
    `(def ~clj-name
       (fixed-map-schema ~name-kw ~key-size ~values-schema))))

(def keyword-schema
  "Lancaster schema object representing a Clojure keyword."
  (let [fields [{:name :namespace
                 :type [:null :string]
                 :default nil}
                {:name :name
                 :type :string
                 :default ""}]
        edn-schema {:name ::keyword
                    :type :record
                    :key-ns-type :none
                    :fields fields
                    :logical-type "keyword"
                    :lt->avro #(hash-map :namespace (namespace %)
                                         :name (name %))
                    :avro->lt #(keyword (:namespace %) (:name %))
                    :lt? keyword?
                    :plumatic-schema s/Keyword}]
    (l/edn->schema edn-schema)))
