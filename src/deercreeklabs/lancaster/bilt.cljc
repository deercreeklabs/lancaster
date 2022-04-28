(ns deercreeklabs.lancaster.bilt
  (:require
   [clojure.string :as str]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]
   [schema.core :as s]))

(defn flex-map->rec [key-ns m]
  {(keyword key-ns "ks") (vec (keys m))
   (keyword key-ns "vs") (vec (vals m))})

(defn rec->flex-map [key-ns r]
  (let [ks (r (keyword key-ns "ks"))
        vs (r (keyword key-ns "vs"))]
    (zipmap ks vs)))

(defn flex-map-schema
  [name-kw logical-type key-schema values-schema valid-k?]
  (schemas/validate-name-kw name-kw)
  (let [key-ns (-> (str (namespace name-kw) "." (name name-kw))
                   (str/replace #"\." "-"))
        key-edn-schema (u/edn-schema key-schema)
        values-edn-schema (u/edn-schema values-schema)
        fields [{:name (keyword key-ns "ks")
                 :type {:type :array
                        :items key-edn-schema}
                 :default []}
                {:name (keyword key-ns "vs")
                 :type {:type :array
                        :items values-edn-schema}
                 :default []}]
        kn->es (fn [n] (-> @u/*__INTERNAL__name->schema n :edn-schema)) #_(u/make-name->edn-schema key-edn-schema)
        vn->es (fn [n] (-> @u/*__INTERNAL__name->schema n :edn-schema)) #_(u/make-name->edn-schema values-edn-schema)
        kp-schema (u/edn-schema->plumatic-schema key-edn-schema #_kn->es)
        vp-schema (u/edn-schema->plumatic-schema values-edn-schema #_vn->es)
        edn-schema {:name name-kw
                    :type :record
                    :fields fields
                    :logical-type logical-type
                    :lt->avro (partial flex-map->rec key-ns)
                    :avro->lt (partial rec->flex-map key-ns)
                    :lt? #(and (map? %) (valid-k? (ffirst %)))
                    :valid-k? valid-k?
                    :k->child-edn-schema (constantly values-edn-schema)
                    :plumatic-schema {kp-schema vp-schema}
                    :default-data {}}]
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
                    :fields fields
                    :logical-type "keyword"
                    :lt->avro #(hash-map :namespace (namespace %)
                                         :name (name %))
                    :avro->lt #(keyword (:namespace %) (:name %))
                    :lt? keyword?
                    :plumatic-schema s/Keyword}]
    (schemas/edn-schema->lancaster-schema edn-schema)))
