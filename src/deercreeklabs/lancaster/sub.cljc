(ns deercreeklabs.lancaster.sub
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster.schemas :as schemas]
   [deercreeklabs.lancaster.utils :as u]))

(defn path-type* [path-entry]
  (cond
   (keyword? path-entry) :keyword
   (string? path-entry) :string
   (int? path-entry) :int
   :else nil))

(defn ->matching-union-child-schema [schema edn-schema path-entry]
  (let [path-type (path-type* path-entry)
        ret (some
                (fn [[i sub]]
                  (let [avro-type (u/avro-type-dispatch-lt sub)]
                    (case [avro-type path-type]
                      [:record :keyword] (-> (u/child-schema schema i)
                                             (u/child-schema path-entry))
                      [:map :string] (-> (u/child-schema schema i)
                                         (u/child-schema))
                      [:array :int] (-> (u/child-schema schema i)
                                        (u/child-schema))
                      ;; Can't be union as per Avro spec disallowing
                      ;; immediately nested unions.
                      (if (= :logical-type avro-type)
                        (when-let [->ces (:k->child-edn-schema edn-schema)]
                          (-> (u/child-schema schema i)
                              (u/child-schema)))
                        nil))))
                (map-indexed vector edn-schema))]
    (if ret
      ret
      (throw
       (ex-info
        (str "No matching schema in union for "
             "key `" path-entry "`.")
        {:schema schema :k path-entry})))))

(defn schema-at-path [schema path]
  (let [child (first path)]
    (if-not child
      schema
      (let [edn-schema (:edn-schema schema)
            child-schema (case (u/avro-type-dispatch-lt edn-schema)
                           :record (u/child-schema schema child)
                           :map (u/child-schema schema)
                           :array (u/child-schema schema)
                           :union (->matching-union-child-schema
                                   schema edn-schema child)
                           :logical-type (u/child-schema schema)
                           (throw
                            (ex-info
                             "Can't get schema at path for non container type."
                             (u/sym-map schema path))))]
        (recur child-schema (rest path))))))

(defn member-schemas [schema]
  (let [{:keys [edn-schema name->edn-schema]} schema
        avro-type (u/get-avro-type edn-schema)
        _ (when-not (= :union avro-type)
            (throw (ex-info (str "The argument to `member-schemas` must be "
                                 "a union schema. Got type `" avro-type "`.")
                            (u/sym-map schema avro-type))))]
    (map (fn [sub] (if (keyword? sub)
                     (sub @u/*__INTERNAL__name->schema)
                     sub))
         (:child-info schema))))

(defn member-schema-at-branch [schema branch-index]
  (let [{:keys [edn-schema name->edn-schema]} schema
        avro-type (u/get-avro-type edn-schema)
        _ (when-not (= :union avro-type)
            (throw
             (ex-info (str "The `schema` argument to `member-schema-at-branch` "
                           "must be a union schema. Got type: `" avro-type "`.")
                      (u/sym-map edn-schema avro-type))))
        _ (when-not (int? branch-index)
            (throw
             (ex-info
              (str "The `branch-index` argument to `member-schema-at-branch` "
                   "must be an integer. Got: `" branch-index "`.")
              (u/sym-map branch-index edn-schema avro-type))))
        _ (when (or (>= branch-index (count edn-schema))
                    (neg? branch-index))
            (throw
             (ex-info
              (str "The `branch-index` argument (`" branch-index "`) "
                   "to `member-schema-at-branch` is out of range. Length of "
                   "union schema is " (count edn-schema) ".")
              (u/sym-map edn-schema branch-index))))
        member-edn-schema (nth edn-schema branch-index)]
    (u/child-schema schema branch-index)))
