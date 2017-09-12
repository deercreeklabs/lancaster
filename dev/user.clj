(ns user
  (:require
   [clojure.pprint :refer [pp pprint]]
   [clojure.string :as str]
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :refer [debugf errorf infof tracef]]))
