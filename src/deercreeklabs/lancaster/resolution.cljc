(ns deercreeklabs.lancaster.resolution
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.pcf :as pcf]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:clj (pm/use-primitive-operators))

(defmulti make-xf (fn [writer-edn-schema reader-edn-schema & args]
                     (let [writer-type (u/get-avro-type writer-edn-schema)
                           reader-type (u/get-avro-type reader-edn-schema)]
                       [writer-type reader-type])))

(defmethod make-xf [:int :long]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (u/int->long data)))

(defmethod make-xf [:int :float]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (float data)))

(defmethod make-xf [:int :double]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (double data)))

(defmethod make-xf [:long :float]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    #?(:clj (float data)
       :cljs (.toNumber data))))

(defmethod make-xf [:long :double]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    #?(:clj (double data)
       :cljs (.toNumber data))))

(defmethod make-xf [:float :double]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (double data)))

(defmethod make-xf [:string :bytes]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (ba/utf8->byte-array data)))

(defmethod make-xf [:bytes :string]
  [writer-edn-schema reader-edn-schema]
  (fn xf [data]
    (ba/byte-array->utf8 data)))

(defmethod make-xf [:array :array]
  [writer-edn-schema reader-edn-schema]
  (let [xf-item (make-xf (:items writer-edn-schema)
                         (:items reader-edn-schema))]
    (fn xf [data]
      (map xf-item data))))

(defn make-resolving-deserializer [writer-pcf reader-schema]
  (let [writer-edn-schema (pcf/pcf->edn-schema writer-pcf)
        reader-edn (u/get-edn-schema reader-schema)
        writer-deserializer (u/make-deserializer writer-edn-schema)
        xf (make-xf writer-edn-schema reader-edn)]
    (fn deserialize [is]
      (xf (writer-deserializer is)))))
