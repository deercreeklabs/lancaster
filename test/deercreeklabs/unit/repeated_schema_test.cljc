(ns deercreeklabs.unit.repeated-schema-test
  (:require
   [clojure.test :refer [deftest is]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [schema.core :as s :include-macros true])
  #?(:clj
     (:import (clojure.lang ExceptionInfo))))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

(l/def-enum-schema month-or-year-schema
  :month :year)

(l/def-record-schema test-schema
  [:f1 month-or-year-schema]
  [:f2 month-or-year-schema])

(deftest test-repeated-enum-schema
  (let [data {:f1 :month
              :f2 :year}
        encoded (l/serialize test-schema data)
        decoded (l/deserialize-same test-schema encoded)]
    (is (= data decoded))))
