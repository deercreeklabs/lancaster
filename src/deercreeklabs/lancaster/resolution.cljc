(ns deercreeklabs.lancaster.resolution
  (:require
   [deercreeklabs.lancaster.pcf :as pcf]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:clj (pm/use-primitive-operators))

(defmulti make-rd* (fn [writer-edn-schema reader-edn-schema & args]
                     (let [writer-type (u/get-avro-type writer-edn-schema)
                           reader-type (u/get-avro-type reader-edn-schema)]
                       [writer-type reader-type])))

(defmethod make-rd* [:int :long]
  [writer-edn-schema reader-edn-schema]
  (let [writer-deserializer (u/make-deserializer writer-edn-schema)]
    (fn deserialize [is]
      (u/int->long (writer-deserializer is)))))

(defmethod make-rd* [:int :float]
  [writer-edn-schema reader-edn-schema]
  (let [writer-deserializer (u/make-deserializer writer-edn-schema)]
    (fn deserialize [is]
      (float (writer-deserializer is)))))

(defmethod make-rd* [:int :double]
  [writer-edn-schema reader-edn-schema]
  (let [writer-deserializer (u/make-deserializer writer-edn-schema)]
    (fn deserialize [is]
      (double (writer-deserializer is)))))

(defmethod make-rd* [:long :float]
  [writer-edn-schema reader-edn-schema]
  (let [writer-deserializer (u/make-deserializer writer-edn-schema)]
    (fn deserialize [is]
      (let [l (writer-deserializer is)]
        #?(:clj (float l)
           :cljs (.toNumber l))))))

(defmethod make-rd* [:long :double]
  [writer-edn-schema reader-edn-schema]
  (let [writer-deserializer (u/make-deserializer writer-edn-schema)]
    (fn deserialize [is]
      (let [l (writer-deserializer is)]
        #?(:clj (double l)
           :cljs (.toNumber l))))))

(defn make-resolving-deserializer [writer-pcf reader-schema]
  (let [writer-avro-schema (pcf/pcf->avro-schema writer-pcf)
        reader-edn (u/get-edn-schema reader-schema)]
    (make-rd* writer-avro-schema reader-edn)))
