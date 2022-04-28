(ns user
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.lancaster.schemas :as s]
   ))

(reset! u/*__INTERNAL__name->schema {})
(println l/string-schema)
(println (keys @u/*__INTERNAL__name->schema))
(println (:com.company.foo-bar/foo-record @u/*__INTERNAL__name->schema))
(println (l/map-schema l/string-schema))
(println (l/map-schema l/int-schema))
(println (s/schema :map nil l/string-schema))

(u/edn-schema->name-kw (:edn-schema (l/map-schema l/int-schema)))

(l/def-record-schema a-schema
  [:a l/string-schema])

(println (l/edn a-schema))

(l/def-record-schema b-schema
  [:b a-schema]
  [:bb l/string-schema])

(l/edn b-schema)
(println b-schema)

(println (:int (deref u/*__INTERNAL__name->schema)))

(u/edn-schema->name-kw (l/edn a-schema))

(l/edn l/int-schema)

(-> (l/array-schema a-schema) (u/edn-schema->name-kw (:type (l/edn a-schema))))
(keys (deref u/*__INTERNAL__name->schema))



