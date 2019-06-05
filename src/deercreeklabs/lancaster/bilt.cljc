(ns deercreeklabs.lancaster.bilt
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [schema.core :as s]))

(defn flex-map->rec [m]
  {:ks (keys m)
   :vs (vals m)})

(defn rec->flex-map [r]
  (let [{:keys [ks vs]} r]
    (zipmap ks vs)))

(defn flex-map-schema
  [logical-type name-kw key-schema values-schema valid-k?]
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
  (flex-map-schema "int-map" name-kw l/int-schema values-schema int?))

(defn long-map-schema
  "Creates a Lancaster schema object representing a map of `long` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw values-schema]
  (flex-map-schema "long-map" name-kw l/long-schema values-schema
                   u/long-or-int?))

(defn fixed-map-schema
  "Creates a Lancaster schema object representing a map of `long` keys
   to values described by the given `values-schema`.
   Differs from map-schema, which only allows string keys."
  [name-kw key-size values-schema]
  (when-not (nat-int? key-size)
    (throw (ex-info (str "Second argument to fixed-map-schema must be a "
                         "positive integer. Got `" key-size "`.")
                    (u/sym-map key-size name-kw))))
  (let [key-schema-name (keyword (namespace name-kw)
                                 (str (name name-kw) "-key"))
        key-schema (l/fixed-schema key-schema-name key-size)]
    (flex-map-schema "fixed-map" name-kw key-schema values-schema
                     ba/byte-array?)))

(defn keyword-schema
  "Creates a Lancaster schema object representing a Clojure keyword."
  [name-kw]
  (let [fields [{:name :namespace
                 :type [:null :string]
                 :default nil}
                {:name :name
                 :type :string
                 :default ""}]
        edn-schema {:name name-kw
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
