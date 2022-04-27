(ns user
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   ))

(l/def-record-schema a-schema
  [:a l/string-schema])
(println *e)

(println a-schema)

(l/def-record-schema b-schema
  [:b a-schema]
  [:bb l/string-schema])

(l/edn b-schema)
(println b-schema)

(println (:int (deref u/*name->schema)))

(u/edn-schema->name-kw (l/edn a-schema))

(l/edn l/int-schema)

(-> (l/array-schema a-schema) (u/edn-schema->name-kw (:type (l/edn a-schema))))
(keys (deref u/*name->schema))



