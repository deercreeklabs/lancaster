(ns user
  (:require
   [cljs.repl]
   [cljs.repl.node]))

(defn node-repl []
  (cljs.repl/repl (cljs.repl.node/repl-env)))
