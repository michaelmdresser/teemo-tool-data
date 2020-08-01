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
  ; these players were used to kickstart the crawler, observed
  ; on the salty teemo stream
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

; each main loop is responsible for one part of the pipeline
; the loops do job queue cleanup (updating timed out jobs and
; closing jobs that have failed beyond the threshold) and then
; get a job and run it through their respective transducer which
; performs all data enrichment and database inserts. Failures are
; handled on a case-by-case basis.

(defn pipeline-main-loop
  [db job-table make-transducer build-job-map]
    (let [transducer (make-transducer db)]
      (while true
        (try+
         (app-db/update-job-queue-for-timed-out db job-table)
         (app-db/close-failed-jobs db job-table)
         (let [job-id (app-db/get-job-from-job-queue db job-table)]
           (if (not (nil? job-id))
             (try+
              (transduce transducer
                         conj
                         [(build-job-map db job-id)]) ; in a vector for transduce, it expects a collection
            (catch [:status 403] {:keys [request-time headers body]}
              ; a 403 will generally be an API key issue
              ; so exit immediately
              (throw+))
            (catch [:status 404] {:keys [request-time headers body]}
              (warn "404 during " job-table " transduce, failing id " job-id)
              (app-db/fail-job db job-table job-id))
            (catch [:status 503] {:keys [request-time headers body]}
              (warn "503, waiting")
              ; (app-db/fail-job db job-table job-id)
              (Thread/sleep 5000))
            (catch [:status 504] {:keys [request-time headers body]}
              (warn "504, waiting")
              ; (app-db/fail-job db job-table job-id)
              (Thread/sleep 5000))
            (catch [:status 400] {:keys [request-time headers body]}
              ; 400s happen due to api quirks or something like that,
              ; but they are error instead of warn because they aren't
              ; expected
              (error "400 during " job-table " transduce, failing id " job-id)
              (app-db/fail-job db job-table job-id))
            (catch Object e
              (error "uncaught exception for transduce on table " job-table " id " job-id)
              (throw+)))
           (do
             (info job-table " loop has no jobs, waiting")
             (Thread/sleep 30000))))
       (catch org.sqlite.SQLiteException e
         (if (clojure.string/includes? (.getMessage e) "SQLITE_BUSY")
           (do
             (debug "SQLITE BUSY, waiting")
             (Thread/sleep 1000))
           (throw+)))))))

(defn main-loop-summ-to-mmr
  [db]
  (pipeline-main-loop db
                      "summoner_data_job_queue"
                      summ-to-mmr/make-summoner-mmr-transducer
                      summ-to-mmr/build-summoner-to-mmr-job-map))

(defn main-loop-mmr-to-match
  [db]
  (pipeline-main-loop db
                      "mmr_data_job_queue"
                      mmr-to-match/make-mmr-to-match-transducer
                      mmr-to-match/build-mmr-to-match-job-map))

(defn main-loop-match-to-summ
  [db]
  (pipeline-main-loop db
                      "match_job_queue"
                      match-to-summ/make-match-to-summoner-transducer
                      match-to-summ/build-match-to-summoner-job-map))

(defn -main
  [& args]
  (let [c1 (async/thread (main-loop-summ-to-mmr db))
        c2 (async/thread (main-loop-mmr-to-match db))
        c3 (async/thread (main-loop-match-to-summ db))]
    (async/<!! c1)
    (async/<!! c2)
    (async/<!! c3)))
