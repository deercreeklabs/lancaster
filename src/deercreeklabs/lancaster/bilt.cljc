(ns deercreeklabs.lancaster.bilt
  (:require
   [deercreeklabs.baracus :as ba]
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
  (let [key-edn-schema (u/edn-schema key-schema)
        values-edn-schema (u/edn-schema values-schema)
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
    (schemas/edn-schema->lancaster-schema edn-schema)))

(def keyword-schema
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
    (schemas/edn-schema->lancaster-schema edn-schema)))
