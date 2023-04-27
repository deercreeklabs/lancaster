(ns user
  (:require
   [deercreeklabs.lancaster :as l]))

(def writer-edn
  {:name :lancaster.schemas/container
   :type :record
   :fields [{:name :things
             :type {:type :array
                    :items {:name :lancaster.schemas/thing
                            :type :record
                            :fields []}}}
            {:name :things-copy
             :type {:type :array
                    :items :lancaster.schemas/thing}}]})


(def reader-edn
  {:name :lancaster.schemas/container
   :type :record
   :fields [{:name :things
             :type {:type :array
                    :items {:name :lancaster.schemas/thing
                            :type :record
                            ; This entry in :fields is the only difference
                            :fields [{:name :new-field
                                      :type [:null :string]
                                      :default nil}]}}}
            {:name :things-copy
             :type {:type :array
                    :items :lancaster.schemas/thing}}]})

(def value {:things [{}]
            :things-copy [{}]})

;; This works
(true?
  (= value
     (l/deserialize (l/edn->schema writer-edn)
                    (l/edn->schema writer-edn)
                    (l/serialize (l/edn->schema writer-edn) value))))

;; This throws
(l/deserialize (l/edn->schema reader-edn)
               (l/edn->schema writer-edn)
               (l/serialize (l/edn->schema writer-edn) value))
