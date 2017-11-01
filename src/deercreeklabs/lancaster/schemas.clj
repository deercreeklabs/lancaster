(ns deercreeklabs.lancaster.schemas
  (:require
   [camel-snake-kebab.core :as csk]
   [cheshire.core :as json]
   [deercreeklabs.lancaster.gen :as gen]
   [deercreeklabs.lancaster.impl :as i]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]))

(defn replace-nil-or-recur-schema [short-name args]
  (clojure.walk/postwalk
   (fn [form]
     (if (= :nil-or-recur form)
       (let [edn-schema [:null short-name]
             record-dispatch-name (str *ns* "." short-name)
             union-dispatch-name (str "union-of-null," record-dispatch-name)
             constructor `(defmethod u/clj->avro ~union-dispatch-name
                            [union-dispatch-name# data#]
                            (when-not (nil? data#)
                              (u/clj->avro ~record-dispatch-name data#)))
             post-converter `(defmethod u/avro->clj ~union-dispatch-name
                               [union-dispatch-name# avro-data#]
                               (when-not (nil? avro-data#)
                                 (u/avro->clj ~record-dispatch-name
                                              avro-data#)))]
         `(do
            ~constructor
            ~post-converter
            ~edn-schema))
       form))
   args))

(defn schema-helper [schema-type clj-var-name args]
  (let [short-name (u/drop-schema-from-name clj-var-name)
        args (cond->> args
               (= :record schema-type) (replace-nil-or-recur-schema
                                        short-name)
               true (eval))
        schema-ns (str *ns*)
        java-ns (namespace-munge schema-ns)
        java-class-name (csk/->PascalCase short-name)
        class-expr (map symbol [java-ns java-class-name])
        full-java-name (str java-ns "." java-class-name)
        edn-schema (u/make-edn-schema schema-type schema-ns short-name args)
        dispatch-name (u/edn-schema->dispatch-name edn-schema)
        constructor (u/make-constructor edn-schema full-java-name dispatch-name)
        post-converter (u/make-post-converter edn-schema full-java-name
                                              dispatch-name)
        avro-schema (u/edn-schema->avro-schema edn-schema)
        json-schema (json/generate-string avro-schema {:pretty true})
        avro-type (u/get-avro-type edn-schema)]
    `(u/if-cljs
      (let [schema-obj# (deercreeklabs.lancaster.impl/make-schema-obj
                         ~edn-schema ~json-schema)]
        (def ~clj-var-name schema-obj#))
      (do
        (when (u/avro-named-types ~avro-type)
          (gen/generate-classes ~json-schema))
        (let [schema-obj# (deercreeklabs.lancaster.impl/make-schema-obj
                           ~dispatch-name ~edn-schema ~json-schema)]
          (when (u/avro-named-types ~avro-type)
            (import ~class-expr))
          (when ~constructor
            ~constructor)
          (when ~post-converter
            ~post-converter)
          (def ~clj-var-name schema-obj#))))))
