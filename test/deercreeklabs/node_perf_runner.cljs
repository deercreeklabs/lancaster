(ns deercreeklabs.node-perf-runner
  (:require
   [cljs.nodejs :as nodejs]
   [cljs.test :as test :refer-macros [run-tests]]
   [deercreeklabs.perf.perf-test]))

(nodejs/enable-util-print!)

(defn -main [& args]
  (run-tests 'deercreeklabs.perf.perf-test))

(set! *main-cli-fn* -main)
