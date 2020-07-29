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

(timbre/set-level! :debug)

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "/home/delta/db/teemo-tool-data.db"})

;(app-db/create-all-tables db)

;(def my-account-id (riot/get-account-id-from-summoner-name "eternal delta" "na1"))

;my-account-id

;(do-account-match-history-get-and-inserts db my-account-id "na1")

(comment
  (def on-stream-players ["eddymartinreal"
                          "crayziehorse"
                          "crimsonban"
                          "broccoli dog"
                          "wsbeast"
                          "pramanikta"
                          "merkonical"
                          "adventslayer"
                          "22ho22"
                          "lizardboy213"])

  (doseq [summoner-name on-stream-players]
    (summ-to-mmr/manual-create-summoner-job-by-summoner-name db summoner-name "na1"))
)

(comment
  ; single job test for summ-to-mmr
(summ-to-mmr/manual-create-summoner-job-by-summoner-name db "floridian demon" "NA1")

(def chan-summ-to-mmr (async/chan 1 (summ-to-mmr/make-summoner-mmr-transducer db)))

(summ-to-mmr/get-and-start-summoner-to-mmr-job db chan-summ-to-mmr)

)

(comment
  ; single job test for mmr-to-match
(mmr-to-match/manual-create-mmr-job-by-summoner-name db "eternal delta" "na1")

(def chan-mmr-to-match (async/chan 1 (mmr-to-match/make-mmr-to-match-transducer db)))

(mmr-to-match/get-and-start-mmr-to-match-job db chan-mmr-to-match)

)

(comment
  ; "single job" but relies on previous test to run
(def chan-match-to-summ (async/chan 1 (match-to-summ/make-match-to-summoner-transducer db)))

(match-to-summ/get-and-start-match-to-summoner-job db chan-match-to-summ)
)

(defn main-loop-summ-to-mmr
  [db]
  (let [cn (async/chan 1 (summ-to-mmr/make-summoner-mmr-transducer db))]
    (while true
      (try+
      (let [result (summ-to-mmr/get-and-start-summoner-to-mmr-job db cn)]
        (when (= result :errnojob)
          ; no jobs available, wait a bit for more
          (Thread/sleep 4000)))
      (catch org.sqlite.SQLiteException e
        (error "exception " e)
        (Thread/sleep 1000))))))

(defn main-loop-mmr-to-match
  [db]
  (let [cn (async/chan 1 (mmr-to-match/make-mmr-to-match-transducer db))]
    (while true
      (try+
      (let [result (mmr-to-match/get-and-start-mmr-to-match-job db cn)]
        (when (= result :errnojob)
          ; no jobs available, wait a bit for more
          (Thread/sleep 4000)))
      (catch org.sqlite.SQLiteException e
        (error "exception " e)
        (Thread/sleep 1000))))))

(defn main-loop-match-to-summ
  [db]
  (let [cn (async/chan 1 (match-to-summ/make-match-to-summoner-transducer db))]
    (while true
      (try+
      (let [result (match-to-summ/get-and-start-match-to-summoner-job db cn)]
        (when (= result :errnojob)
          ; no jobs available, wait a bit for more
          (Thread/sleep 4000)))
      (catch org.sqlite.SQLiteException e
        (error "exception " e)
        (Thread/sleep 1000))))))


(defn -main
  [& args]
  (let [c1 (async/thread (main-loop-summ-to-mmr db))
        c2 (async/thread (main-loop-mmr-to-match db))
        c3 (async/thread (main-loop-match-to-summ db))]
    (async/<!! c1)
    (async/<!! c2)
    (async/<!! c3)))
