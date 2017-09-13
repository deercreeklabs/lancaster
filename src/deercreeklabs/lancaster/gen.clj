(ns deercreeklabs.lancaster.gen
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.tools.analyzer.jvm :as taj]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  (:import
   (clojure.lang Namespace)
   (java.io File PrintStream)
   (org.apache.avro.tool SpecificCompilerTool)))

(def excluded-top-level-namespaces
  #{"camel-snake-kebab" "cheshire" "cider" "clj-time" "cljs-time" "clojure"
    "leiningen" "user"})

(defn get-relevant-namespaces []
  (let [relevant? (fn [^Namespace ns]
                    (not (excluded-top-level-namespaces
                          (first (clojure.string/split
                                  (name (.getName ns)) #"\.")))))]
    (filter relevant? (all-ns))))

(defn get-named-schemas-in-ns [^Namespace ns]
  (let [ast (taj/analyze-ns ns)]
    (keep (fn [node]
            (when (or (get-in node [:meta :form :avro-schema])
                      (get-in node [:meta :val :avro-schema]))
              (var-get (:result node))))
          ast)))

(defn get-named-schemas []
  (vec (mapcat get-named-schemas-in-ns (get-relevant-namespaces))))


(defn make-temp-dir []
  (let [dir-name (clojure.string/join "/"
                                      [(System/getProperty "java.io.tmpdir")
                                       "avro_schemas"
                                       (str (java.util.UUID/randomUUID))])
        ^File dir (File. dir-name)]
    (.mkdirs dir)
    dir))

(defn write-classes [in-path out-path]
  (let [tool (SpecificCompilerTool.)
        n (rand-int 1000)
        ;; TODO: Throw an exception if err gets written to
        err nil
        args ["schema" in-path out-path]]
    (.run tool nil nil err args)))

(defn remove-dir [^String dir-path]
  (shell/sh "rm" "-rf" dir-path))

(defn write-avsc-files
  [schemas]
  (let [
        ^File dir (make-temp-dir)
        dir-path (.getAbsolutePath dir)
        files (doall
               (map-indexed (fn [i schema]
                              (let [f (str dir-path "/" i ".avsc")]
                                (spit f (json/generate-string schema
                                                              {:pretty true}))
                                f))
                            schemas))]
    dir-path))

(defn gen-classes
  ([out-dir]
   (gen-classes nil out-dir))
  ([ns out-dir]
   (let [schemas (if ns
                   (get-named-schemas-in-ns ns)
                   (get-named-schemas))
         dir-path (write-avsc-files schemas)]
     (try
       (let [ret (write-classes dir-path out-dir)]
         (when (not= 0 ret)
           (throw (ex-info "Writing classes failed."
                           {:type :execution-error
                            :subtype :writing-avro-classes-failed
                            :ret ret})))
         true)
       (catch Exception e
         (errorf "Error in gen-classes: %s" e))
       (finally
         (remove-dir dir-path))))))
