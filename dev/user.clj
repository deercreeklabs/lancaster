(ns user
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   ))

(l/def-record-schema a-schema
  [:a l/string-schema])

(println a-schema)

(l/def-record-schema b-schema
  [:b a-schema]
  [:bb l/string-schema])

(l/edn b-schema)
(println b-schema)

(println (:int (deref u/*name->schema)))

(-> (l/array-schema a-schema) (l/edn))



