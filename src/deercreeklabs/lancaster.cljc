(ns deercreeklabs.lancaster
  (:require
   [camel-snake-kebab.core :as csk]
   [deercreeklabs.lancaster.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster)))

#?(:cljs
   (set! *warn-on-infer* true))


(defn avro-rec
  [schema-ns schema-name fields]
  (let [make-field (fn [[field-name field-type field-default]]
                     {:name (csk/->camelCase (name field-name))
                      :type (u/get-schema-name field-type)
                      :default (u/get-field-default field-type field-default)})]
    (-> (u/make-named-schema* schema-ns schema-name)
        (assoc :type :record)
        (assoc :fields (mapv make-field fields)))))

(defn avro-enum
  [schema-ns schema-name symbols]
  (let [make-enum-symbol (fn [sym]
                           (-> (name sym)
                               (csk/->SCREAMING_SNAKE_CASE)))]
    (-> (u/make-named-schema* schema-ns schema-name)
        (assoc :type :enum)
        (assoc :symbols (mapv make-enum-symbol symbols)))))

(defn avro-fixed
  [schema-ns schema-name size]
  (-> (u/make-named-schema* schema-ns schema-name)
      (assoc :type :fixed)
      (assoc :size size)))

(defn avro-union [elements]
  (mapv u/get-schema-name elements))

(defn avro-map [values-schema]
  {:type :map
   :values (u/get-schema-name values-schema)})

(defn avro-array [items-schema]
  {:type :array
   :values (u/get-schema-name items-schema)})

;;;;;;;;;;;;;;;;;;;; Macros ;;;;;;;;;;;;;;;;;;;;

(defmacro def-avro-rec
  [schema-name & fields]
  `(u/named-schema-helper* avro-rec ~schema-name ~fields))

(defmacro def-avro-enum
  [schema-name & symbols]
  `(u/named-schema-helper* avro-enum ~schema-name ~symbols))

(defmacro def-avro-fixed
  [schema-name size]
  `(u/named-schema-helper* avro-fixed ~schema-name ~size))
