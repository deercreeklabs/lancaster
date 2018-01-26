(ns deercreeklabs.doo-test-runner
  (:require
   [doo.runner :refer-macros [doo-tests]]
   [deercreeklabs.perf-test]))

(enable-console-print!)

(doo-tests 'deercreeklabs.perf-test)
