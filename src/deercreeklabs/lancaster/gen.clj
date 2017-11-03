(ns deercreeklabs.lancaster.gen
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :refer [join split]]
   [deercreeklabs.log-utils :as lu]
   [me.raynes.fs :as fs]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  (:import
   (clojure.lang Namespace)
   (java.io ByteArrayOutputStream File PrintStream)
   (java.util Arrays)
   (javax.tools StandardJavaFileManager StandardLocation ToolProvider)
   (org.apache.avro.tool SpecificCompilerTool)))


(defn make-temp-dir []
  (let [^File dir (io/file (System/getProperty "java.io.tmpdir")
                          "avro_schemas"
                          (str (java.util.UUID/randomUUID)))]
    (.mkdirs dir)
    dir))

(defn write-sources [in-path out-path]
  (let [tool (SpecificCompilerTool.)
        n (rand-int 1000)
        ;; TODO: Prevent output to console
        ;; TODO: Throw an exception if err gets written to
        err-stream (ByteArrayOutputStream.)
        eps (PrintStream. err-stream true)
        out-stream (ByteArrayOutputStream.)
        ops (PrintStream. out-stream true)
        orig-out System/out
        args ["schema" in-path out-path]
        _ (System/setOut ops)
        ret (.run tool nil nil eps args)
        _ (System/setOut orig-out)
        err-str (.toString err-stream)]
    (when-not (zero? (count err-str))
      (throw (ex-info (str "Error compiling Avro Java classes: " err-str))))
    ret))

(defn compile-classes [java-src-dir]
  (let [root (io/file java-src-dir)
        src-files (filter fs/file? (fs/find-files root #".*java$"))
        compiler (ToolProvider/getSystemJavaCompiler)
        ^StandardJavaFileManager file-mgr (.getStandardFileManager
                                           compiler nil nil nil)
        ^File out-dir (io/file "target" "classes")
        _ (.mkdirs out-dir)
        _ (.setLocation file-mgr StandardLocation/CLASS_OUTPUT [out-dir])
        comp-units (.getJavaFileObjectsFromFiles file-mgr src-files)
        comp-task (.getTask compiler nil file-mgr nil nil nil comp-units)]
    (.call comp-task)
    (.close file-mgr)))

(defn remove-dir [^String dir-path]
  (shell/sh "rm" "-rf" dir-path))

(defn write-avsc-file
  [schema]
  (let [^File dir (make-temp-dir)
        dir-path (.getAbsolutePath dir)
        avsc-filename (str dir-path "/schema.avsc")]
    (spit avsc-filename schema)
    dir-path))

(defn generate-classes [json-schema]
  (let [dir-path (write-avsc-file json-schema)]
    (try
      (let [ret (write-sources dir-path dir-path)]
        (when (not= 0 ret)
          (throw (ex-info "Writing classes failed."
                          {:type :execution-error
                           :subtype :writing-avro-classes-failed
                           :ret ret})))
        (compile-classes dir-path))
      (catch Exception e
        (errorf "Error in generate-class: \n%s"
                (lu/get-exception-msg-and-stacktrace e)))
      (finally
        (remove-dir dir-path)))))
