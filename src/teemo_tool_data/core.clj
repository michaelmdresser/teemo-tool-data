(ns teemo-tool-data.core
  (:gen-class)
  (:require [clj-http.client :as client]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as sql]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [slingshot.slingshot :refer [try+ throw+]]
            [teemo-tool-data.db :as app-db]
            [teemo-tool-data.mmr :as mmr]
            [teemo-tool-data.riot :as riot]
            [teemo-tool-data.transducers.summoner-to-mmr :as summ-to-mmr]
            [teemo-tool-data.transducers.mmr-to-match :as mmr-to-match]
            [teemo-tool-data.transducers.match-to-summoner :as match-to-summ]
            ))

(timbre/set-level! :trace)

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "/home/delta/db/teemo-tool-data.db"})

;(app-db/create-all-tables db)

;(def my-account-id (riot/get-account-id-from-summoner-name "eternal delta" "na1"))

;my-account-id

;(do-account-match-history-get-and-inserts db my-account-id "na1")

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
