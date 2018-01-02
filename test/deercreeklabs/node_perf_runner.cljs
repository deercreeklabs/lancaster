(ns deercreeklabs.node-perf-runner
  (:require
   [cljs.nodejs :as nodejs]
   [cljs.test :as test :refer-macros [run-tests]]
   [deercreeklabs.perf-test]))

(nodejs/enable-util-print!)

(defn -main [& args]
  (.install (js/require "source-map-support"))
  (run-tests 'deercreeklabs.perf-test))

(set! *main-cli-fn* -main)
