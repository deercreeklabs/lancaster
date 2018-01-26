(ns deercreeklabs.lancaster.resolution
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [primitive-math :as pm])
   [schema.core :as s :include-macros true]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:clj (pm/use-primitive-operators))

(defn make-resolving-deserializer [reader-schema writer-pcf]
  (let [reader-edn (u/get-edn-schema reader-schema)]
    (fn [input-stream]
      :foo)))
