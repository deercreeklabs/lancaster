(ns deercreeklabs.lancaster.utils
  (:require
   [camel-snake-kebab.core :as csk]
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   [deercreeklabs.log-utils :as lu]
   #?(:clj [puget.printer :refer [cprint]])
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster.utils)))

#?(:cljs
   (set! *warn-on-infer* true))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn}))

(defn byte-array->byte-str [ba]
  (apply str (map char ba)))
