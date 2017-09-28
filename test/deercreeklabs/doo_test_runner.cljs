(ns deercreeklabs.doo-test-runner
  (:require
   [doo.runner :refer-macros [doo-tests]]
   [deercreeklabs.lancaster-test]))

(enable-console-print!)

(doo-tests 'deercreeklabs.lancaster-test)
