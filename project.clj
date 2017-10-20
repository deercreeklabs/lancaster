(defproject deercreeklabs/lancaster "0.1.1"
  :description "Tools for working with Apache Avro"
  :url "http://www.deercreeklabs.com"
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
    [[lein-ancient "0.6.12"]
     [lein-cljsbuild "1.1.7" :exclusions [org.clojure/clojure]]
     [lein-cloverage "1.0.9" :exclusions [org.clojure/clojure]]
     [lein-doo "0.1.8"]
     [lein-npm "0.6.2" :exclusions [com.fasterxml.jackson.core/jackson-core]]
     ;; Because of confusion with a defunct project also called
     ;; lein-release, we exclude lein-release from lein-ancient.
     [lein-release "1.0.9" :upgrade false :exclusions [org.clojure/clojure]]]
    :dependencies
    [[doo "0.1.8"]
     [org.clojure/tools.namespace "0.2.11"]]}}

  :npm {:dependencies [[avsc "5.0.5"]]
        :devDependencies [[karma "1.7.1"]
                          [karma-chrome-launcher "2.2.0"]
                          [karma-cljs-test "0.1.0"]
                          [karma-firefox-launcher "1.0.1"]
                          [source-map-support "0.4.17"]]}

  :dependencies
  [[binaryage/oops "0.5.6"]
   [camel-snake-kebab "0.4.0"]
   [cheshire "5.8.0"]
   [cljsjs/long "3.0.3-1"]
   [com.taoensso/timbre "4.10.0"]
   [deercreeklabs/baracus "0.1.0" :exclusions [prismatic/schema]]
   [deercreeklabs/log-utils "0.1.0"]
   [deercreeklabs/stockroom "0.1.11"]
   [me.raynes/fs "1.4.6" :exclusions [org.apache.commons/commons-compress]]
   [org.apache.avro/avro "1.8.2" :exclusions [org.slf4j/slf4j-api]]
   [org.apache.avro/avro-tools "1.8.2" :exclusions [commons-logging]]
   [org.clojure/clojure "1.9.0-beta2"]
   [org.clojure/clojurescript "1.9.946"]]

  :cljsbuild
  {:builds
   [{:id "node-test-none"
     :source-paths ["src" "test"]
     :notify-command ["node" "target/test/node_test_none/test_main.js"]
     :compiler
     {:optimizations :none
      :parallel-build true
      :main "deercreeklabs.node-test-runner"
      :target :nodejs
      :output-to "target/test/node_test_none/test_main.js"
      :output-dir "target/test/node_test_none"
      :source-map true}}
    {:id "node-test-adv"
     :source-paths ["src" "test"]
     :notify-command ["node" "target/test/node_test_adv/test_main.js"]
     :compiler
     {:optimizations :advanced
      ;;:pseudo-names true
      ;;:pretty-print true
      ;;:infer-externs true
      :externs ["lancaster_externs.js"]
      :parallel-build true
      :main "deercreeklabs.node-test-runner"
      :target :nodejs
      :static-fns true
      :output-to  "target/test/node_test_adv/test_main.js"
      :output-dir "target/test/node_test_adv"
      :source-map "target/test/node_test_adv/map.js.map"}}
    {:id "node-test-simple"
     :source-paths ["src" "test"]
     :notify-command ["node" "target/test/node_test_simple/test_main.js"]
     :compiler
     {:optimizations :simple
      :parallel-build true
      :main "deercreeklabs.node-test-runner"
      :target :nodejs
      :static-fns true
      :output-to  "target/test/node_test_simple/test_main.js"
      :output-dir "target/test/node_test_simple"
      :source-map "target/test/node_test_simple/map.js.map"}}
    {:id "doo-test-none"
     :source-paths ["src" "test"]
     :compiler
     {:optimizations :none
      :parallel-build true
      :main "deercreeklabs.doo-test-runner"
      :output-to "target/test/doo_test_none/test_main.js"
      :output-dir "target/test/doo_test_none"
      :source-map true}}
    {:id "doo-test-simple"
     :source-paths ["src" "test"]
     :compiler
     {:optimizations :simple
      :parallel-build true
      :main "deercreeklabs.doo-test-runner"
      :output-to "target/test/doo_test_simple/test_main.js"
      :output-dir "target/test/doo_test_simple"
      :source-map "target/test/doo_test_simple/map.js.map"}}
    {:id "doo-test-adv"
     :source-paths ["src" "test"]
     :compiler
     {:optimizations :advanced
      ;; :pseudo-names true
      ;; :pretty-print true
      ;; :infer-externs true
      :externs ["lancaster_externs.js"]
      :parallel-build true
      :main "deercreeklabs.doo-test-runner"
      :static-fns true
      :output-to  "target/test/doo_test_adv/test_main.js"
      :output-dir "target/test/doo_test_adv"
      :source-map "target/test/doo_test_adv/map.js.map"}}
    {:id "build-simple"
     :source-paths ["src"]
     :compiler
     {:optimizations :simple
      :parallel-build true
      :static-fns true
      :output-to  "target/build_simple/tube.js"
      :output-dir "target/build_simple"
      :source-map "target/build_simple/map.js.map"}}]}

  :aliases
  {"auto-test-cljs" ["do"
                     "clean,"
                     "cljsbuild" "auto" "node-test-none"]
   "auto-test-cljs-adv" ["do"
                         "clean,"
                         "cljsbuild" "auto" "node-test-adv"]
   "auto-test-cljs-simple" ["do"
                            "clean,"
                            "cljsbuild" "auto" "node-test-simple"]
   "build-simple" ["do"
                   "clean,"
                   "cljsbuild" "once" "build-simple"]
   "chrome-test" ["do"
                  "clean,"
                  "doo" "chrome" "doo-test-adv"]})
