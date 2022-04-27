(ns user
  (:require
   [deercreeklabs.lancaster :as l]
   ))

(l/def-record-schema a-schema
  [:a l/string-schema])

(l/def-record-schema b-schema
  [:b a-schema]
  [:bb l/string-schema])

(l/edn b-schema)
(-> (l/array-schema a-schema) (l/edn))



