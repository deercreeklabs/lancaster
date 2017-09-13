(ns deercreeklabs.lancaster.gen
  (:require
   [camel-snake-kebab.core :as csk]
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :refer [join split]]
   [clojure.tools.analyzer.jvm :as taj]
   [deercreeklabs.lancaster.utils :as u]
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
                          (first (split (name (.getName ns)) #"\.")))))]
    (filter relevant? (all-ns))))

(defn get-named-schemas-in-ns [^Namespace ns]
  (let [ast (taj/analyze-ns ns)]
    (keep (fn [node]
            (when (or (get-in node [:meta :form :avro-schema])
                      (get-in node [:meta :val :avro-schema]))
              {:name (name (:name node))
               :val (var-get (:result node))}))
          ast)))

(defn get-named-schemas []
  (mapcat get-named-schemas-in-ns (get-relevant-namespaces)))


(defn make-temp-dir []
  (let [dir-name (join "/"
                       [(System/getProperty "java.io.tmpdir")
                        "avro_schemas"
                        (str (java.util.UUID/randomUUID))])
        ^File dir (File. dir-name)]
    (.mkdirs dir)
    dir))

(defn write-classes [in-path out-path]
  (let [tool (SpecificCompilerTool.)
        n (rand-int 1000)
        ;; TODO: Prevent output to console
        ;; TODO: Throw an exception if err gets written to
        err nil
        args ["schema" in-path out-path]]
    (.run tool nil nil err args)))

(defn remove-dir [^String dir-path]
  (shell/sh "rm" "-rf" dir-path))

(defn write-avsc-files
  [schemas]
  (let [^File dir (make-temp-dir)
        dir-path (.getAbsolutePath dir)
        files (doall
               (map-indexed (fn [i schema]
                              (let [f (str dir-path "/" i ".avsc")]
                                (spit f (json/generate-string
                                         (:val schema) {:pretty true}))
                                f))
                            schemas))]
    dir-path))

(defn write-lines [path lines]
  (spit path (join "\n" lines)))

(defn make-enum-constructor-lines [schema]
  (let [obj-name (u/drop-schema-from-name (:name schema))
        class-name (-> schema :val :name)
        symbols (-> schema :val :symbols)
        def-line (format  "(defn make-%s [symbol-str]" obj-name)
        case-line (format "  (case symbol-str")
        sym-lines (map #(format "    \"%s\" %s/%s" % class-name %)
                       symbols)
        sym-lines (concat (butlast sym-lines)
                          [(str (last sym-lines) "))\n")])]
    (concat [def-line case-line] sym-lines)))

(defn make-record-constructor-lines [schema]
  (let [obj-name (u/drop-schema-from-name (:name schema))
        class-name (-> schema :val :name)
        fields (map :name (-> schema :val :fields))
        args (map #(csk/->kebab-case %) fields)
        def-line (format "(defn make-%s [%s]"
                         obj-name (join " " args))
        body-header (format "  (cond-> (%s/newBuilder)" class-name)
        body-lines (map #(let [arg (csk/->kebab-case %)]
                           (format "    %s (.set%s %s)" arg
                                   (csk/->PascalCase %) arg))
                    fields)
        body-lines (concat (butlast body-lines)
                           [(str (last body-lines) "))\n")])]
    (concat [def-line body-header] body-lines)))

(defn make-constructor-lines [schema]
  (let [f (case (-> schema :val :type)
            :record make-record-constructor-lines
            :fixed (constantly [])
            :enum make-enum-constructor-lines)]
    (f schema)))

(defn make-ns-lines [schemas]
  (let [ns (-> schemas first :val :namespace)
        header (format "(ns %s\n  (:import\n   (%s" ns ns)
        ns-decl-footer ")))\n"
        imports (map #(->> % :val :name (str "      "))
                     schemas)
        imports (concat (butlast imports) [(str (last imports) ns-decl-footer)])
        constructors (mapcat make-constructor-lines schemas)]
    (concat [header] imports constructors)))

(defn gen-constructor [schemas out-dir]
  (let [ns-parts (-> (first schemas)
                     (:val)
                     (:namespace)
                     (split #"\."))
        fname (str (last ns-parts) ".cljc")
        ^File parent-dir (apply io/file (concat [out-dir] (butlast ns-parts)))
        _ (.mkdirs parent-dir)
        ^File f (io/file parent-dir fname)
        lines (make-ns-lines schemas)]
    (write-lines (.getAbsolutePath f) lines)))

(defn gen-constructors [ns out-dir]
  (let [schemas (if ns
                  (get-named-schemas-in-ns ns)
                  (get-named-schemas))
        ns-groups (group-by #(-> % :val :namespace) schemas)]
    (doseq [[ns group] ns-groups]
      (gen-constructor group out-dir))
    true))

(defn gen-classes-and-constructors
  ([java-out-dir clj-out-dir]
   (gen-classes-and-constructors nil java-out-dir clj-out-dir))
  ([ns java-out-dir clj-out-dir]
   (let [schemas (if ns
                   (get-named-schemas-in-ns ns)
                   (get-named-schemas))
         dir-path (write-avsc-files schemas)]
     (try
       (let [ret (write-classes dir-path java-out-dir)]
         (when (not= 0 ret)
           (throw (ex-info "Writing classes failed."
                           {:type :execution-error
                            :subtype :writing-avro-classes-failed
                            :ret ret})))
         (gen-constructors ns clj-out-dir))
       (catch Exception e
         (errorf "Error in gen-classes: %s" e))
       (finally
         (remove-dir dir-path))))))
