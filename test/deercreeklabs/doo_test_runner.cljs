(ns deercreeklabs.doo-test-runner
  (:require
   [doo.runner :refer-macros [doo-tests]]
   [deercreeklabs.tools-test]))

(enable-console-print!)

(doo-tests 'deercreeklabs.tools-test)
