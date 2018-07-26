(def compiler-defaults
  {:npm-deps {:pako "1.0.6"
              :source-map-support "0.5.6"}
   :install-deps true
   :parallel-build true
   :static-fns true
   ;; :pseudo-names true
   ;; :pretty-print true
   ;; :infer-externs true
   :externs ["lancaster_externs.js"]})

(defn make-build-conf [id target-kw build-type-kw opt-level main]
  (let [build-type-str (name build-type-kw)
        target-str (if target-kw
                     (name target-kw)
                     "")
        node? (= :node target-kw)
        source-paths (case build-type-kw
                       :build ["src"]
                       :test ["src" "test"]
                       :perf ["src" "test"])
        build-name (str target-str "_" build-type-str "_" (name opt-level))
        output-name (case build-type-kw
                      :build "main.js"
                      :test "test_main.js"
                      :perf "perf_main.js")
        output-dir (str "target/" build-type-str "/" build-name)
        output-to (str output-dir "/" output-name)
        source-map (if (= :none opt-level)
                     true
                     (str output-dir "/map.js.map"))
        compiler (cond-> compiler-defaults
                   true (assoc :optimizations opt-level
                               :output-to output-to
                               :output-dir output-dir
                               :source-map source-map)
                   main (assoc :main main)
                   node? (assoc :target :nodejs))
        node-test? (and node? (or (= :test build-type-kw)
                                  (= :perf build-type-kw)))]
    (cond-> {:id id
             :source-paths source-paths
             :compiler compiler}
      node-test? (assoc :notify-command ["node" output-to]))))

(defproject deercreeklabs/lancaster "0.4.9-SNAPSHOT"
  :description "Tools for working with Apache Avro"
  :url "https://github.com/deercreeklabs/lancaster"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :lein-release {:scm :git
                 :deploy-via :clojars}

  :pedantic? :abort

  :profiles
  {:dev
   {:global-vars {*warn-on-reflection* true}
    :source-paths ["dev" "src"]
    :repl-options {:init-ns user}
    :plugins
    [[lein-ancient "0.6.15"]
     [lein-cljsbuild "1.1.7" :exclusions [org.clojure/clojure]]
     [lein-cloverage "1.0.11" :exclusions [fipp org.clojure/clojure]]
     [lein-doo "0.1.10"
      :exclusions [org.clojure/clojure org.clojure/clojurescript]]
     ;; Because of confusion with a defunct project also called
     ;; lein-release, we exclude lein-release from lein-ancient.
     [lein-release "1.0.9" :upgrade false :exclusions [org.clojure/clojure]]]
    :dependencies
    [[doo "0.1.10"]
     [org.apache.avro/avro "1.8.2"]
     [org.clojure/tools.namespace "0.2.11"]]}}

  :dependencies
  [[camel-snake-kebab "0.4.0"]
   [cheshire "5.8.0"]
   [com.taoensso/timbre "4.10.0"]
   [deercreeklabs/baracus "0.1.11"]
   [deercreeklabs/log-utils "0.1.8"]
   [org.clojure/clojure "1.9.0"]
   [org.clojure/clojurescript "1.10.339"]
   [primitive-math "0.1.6"]
   [prismatic/schema "1.1.9"]]

  :test-selectors {:default (complement :perf)
                   :perf :perf
                   :the-one :the-one
                   :all (constantly true)}

  :cljsbuild
  {:builds
   [~(make-build-conf "node-test-none" :node :test :none
                      "deercreeklabs.node-test-runner")
    ~(make-build-conf "node-test-adv" :node :test :advanced
                      "deercreeklabs.node-test-runner")
    ~(make-build-conf "node-test-perf" :node :perf :advanced
                      "deercreeklabs.node-perf-runner")
    ~(make-build-conf "doo-test-none" :doo :test :none
                      "deercreeklabs.doo-test-runner")
    ~(make-build-conf "doo-test-adv" :doo :test :advanced
                      "deercreeklabs.doo-test-runner")
    ~(make-build-conf "doo-test-perf" :doo :perf :advanced
                      "deercreeklabs.doo-perf-runner")
    ~(make-build-conf "build-adv" nil :build :advanced nil)]}

  :aliases
  {"auto-test-cljs" ["do"
                     "clean,"
                     "cljsbuild" "auto" "node-test-none"]
   "auto-test-cljs-adv" ["do"
                         "clean,"
                         "cljsbuild" "auto" "node-test-adv"]
   "auto-test-cljs-perf" ["do"
                          "clean,"
                          "cljsbuild" "auto" "node-test-perf"]
   "chrome-test" ["do"
                  "clean,"
                  "doo" "chrome" "doo-test-adv"]
   "chrome-perf" ["do"
                  "clean,"
                  "doo" "chrome" "doo-test-perf"]})
