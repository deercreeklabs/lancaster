(ns deercreeklabs.node-test-runner
  (:require
   [cljs.nodejs :as nodejs]
   [cljs.test :as test :refer-macros [run-tests]]
   [deercreeklabs.unit.lancaster-test]))

(nodejs/enable-util-print!)

(defn -main [& args]
  (.install (js/require "source-map-support"))
  (run-tests 'deercreeklabs.unit.lancaster-test))

(set! *main-cli-fn* -main)
