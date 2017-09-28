(ns deercreeklabs.lancaster.utils
  (:require
   [#?(:clj clj-time.format :cljs cljs-time.format) :as f]
   [#?(:clj clj-time.core :cljs cljs-time.core) :as t]
   #?(:clj [puget.printer :refer [cprint]])
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      deercreeklabs.lancaster.utils)))

#?(:cljs
   (set! *warn-on-infer* true))


(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn short-log-output-fn [data]
  (let [{:keys [level msg_ ?ns-str ?file ?line]} data
        formatter (f/formatters  :hour-minute-second-ms)
        timestamp (f/unparse formatter (t/now))]
    (str
     timestamp " "
     (clojure.string/upper-case (name level))  " "
     "[" (or ?ns-str ?file "?") ":" (or ?line "?") "] - "
     @msg_)))

(defn get-exception-msg
  [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(defn get-exception-stacktrace
  [e]
  #?(:clj (clojure.string/join "\n" (map str (.getStackTrace ^Exception e)))
     :cljs (.-stack e)))

(defn get-exception-msg-and-stacktrace
  [e]
  (str "\nException:\n"
       (get-exception-msg e)
       "\nStacktrace:\n"
       (get-exception-stacktrace e)))

(defn log-exception [e]
  (errorf (get-exception-msg-and-stacktrace e)))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn short-log-output-fn}))

(defn get-current-time-ms
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

;; We have to name this pprint* to not conflict with
;; clojure.repl/pprint, which gets loaded into the repl's namespace
(defn pprint*
  "Pretty-prints its argument. Color is used in clj, but not cljs."
  [x]
  (#?(:clj cprint :cljs cljs.pprint/pprint) x)
  nil)

(defn pprint-str
  "Like pprint, but returns a string."
  [x]
  (with-out-str
    (pprint* x)))
