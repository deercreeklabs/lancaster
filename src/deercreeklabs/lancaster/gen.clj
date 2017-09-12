(ns deercreeklabs.lancaster.gen
  (:require
   [cheshire.core :as json]
   [clojure.java.shell :as shell]
   [clojure.tools.analyzer.jvm :as taj]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  (:import
   (clojure.lang Namespace)
   (java.io File)))

(def excluded-top-level-namespaces
  #{"camel-snake-kebab" "cheshire" "cider" "clj-time" "cljs-time" "clojure"
    "leiningen" "user"})

(defn get-relevant-namespaces []
  (let [relevant? (fn [^Namespace ns]
                    (not (excluded-top-level-namespaces
                          (first (clojure.string/split
                                  (name (.getName ns)) #"\.")))))]
    (filter relevant? (all-ns))))

(defn get-schemas-in-ns [^Namespace ns]
  (let [ast (taj/analyze-ns ns)]
    (keep (fn [node]
            (when (or (get-in node [:meta :form :avro-schema])
                      (get-in node [:meta :val :avro-schema]))
              (var-get (:result node))))
          ast)))

(defn get-schemas []
  (vec (mapcat get-schemas-in-ns (get-relevant-namespaces))))


(defn make-temp-dir []
  (let [dir-name (clojure.string/join "/"
                                      [(System/getProperty "java.io.tmpdir")
                                       "avro_schemas"
                                       (str (java.util.UUID/randomUUID))])
        ^File dir (File. dir-name)]
    (.mkdirs dir)
    dir))

(defn write-avsc-files
  "Returns directory as string."
  ([]
   (write-avsc-files nil))
  ([ns]
   (let [schemas (if ns
                   (get-schemas-in-ns ns)
                   (get-schemas))
         ^File dir (make-temp-dir)
         dir-path (.getAbsolutePath dir)]
     (debugf "# of schemas: %s" (count schemas))
     (doall
      (map-indexed (fn [i schema]
                     (let [f (str dir-path "/" i ".avsc")]
                       (debugf "@@@ f: %s" f)
                       (spit f (json/generate-string schema
                                                     {:pretty true}))))
                   schemas))
     dir-path)))

(defn remove-dir [^String dir-path]
  (shell/sh (str "rm -rf " dir-path)))
