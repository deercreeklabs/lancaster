(ns user
  (:require
   [clojure.pprint :refer [pp pprint]]
   [clojure.string :as str]
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [deercreeklabs.avro-tools :as at]
   [taoensso.timbre :refer [debugf errorf infof tracef]]))
