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
  (let [transducer (summ-to-mmr/make-summoner-mmr-transducer db)
        job-table "summoner_data_job_queue"]
    (while true
      (try+
       (app-db/update-job-queue-for-timed-out db job-table)
       (app-db/close-failed-jobs db job-table)
       (let [job-id (app-db/get-job-from-job-queue db job-table)]
         (if (not (nil? job-id))
           (try+
            (transduce transducer
                       conj
                       [(summ-to-mmr/build-summoner-to-mmr-job-map db job-id)])
            (catch [:status 403] {:keys [request-time headers body]}
              (throw+))
            (catch [:status 404] {:keys [request-time headers body]}
              (error "404 during summ-to-mmr transduce, failing on table " job-table " id " job-id)
              (app-db/fail-job db job-table job-id))
            (catch [:status 503] {:keys [request-time headers body]}
              (error "503, waiting")
              ; (app-db/fail-job db job-table job-id)
              (Thread/sleep 5000))
            (catch [:status 504] {:keys [request-time headers body]}
              (error "504, waiting")
              ; (app-db/fail-job db job-table job-id)
              (Thread/sleep 5000))
            (catch Object e
              (error "non-403 during summ-to-mmr transduce: " e " failing on table " job-table " id " job-id)
              (throw+)))
           (do
             (info "summ-to-mmr loop has no jobs, waiting")
             (Thread/sleep 30000))))
       (catch org.sqlite.SQLiteException e
                                        ; probably a lock held
         (error "sqlite exception " e)
         (if (clojure.string/includes? (.getMessage e) "SQLITE_BUSY")
           (do
             (debug "SQLITE BUSY, waiting")
             (Thread/sleep 1000))
           (throw+)))))))


(defn main-loop-mmr-to-match
  [db]
  (let [transducer (mmr-to-match/make-mmr-to-match-transducer db)
        job-table "mmr_data_job_queue"]
    (while true
      (try+
      (app-db/update-job-queue-for-timed-out db job-table)
      (app-db/close-failed-jobs db job-table)
      (let [job-id (app-db/get-job-from-job-queue db job-table)]
        (if (not (nil? job-id))
          (try+
           (transduce transducer
                      conj
                      [(mmr-to-match/build-mmr-to-match-job-map db job-id)])
           (catch [:status 403] {:keys [request-time headers body]}
             (throw+))
           (catch [:status 503] {:keys [request-time headers body]}
             (error "503, waiting")
             ;(app-db/fail-job db job-table job-id)
             (Thread/sleep 5000))
           (catch [:status 504] {:keys [request-time headers body]}
             (error "504, waiting")
             ;(app-db/fail-job db job-table job-id)
             (Thread/sleep 5000))
           (catch Object e
             (error "non-403 during mmr-to-match transduce: " e " failing on table " job-table " id " job-id)
             (throw+)
             (app-db/fail-job db job-table job-id)))
          (do
            (info "mmr-to-match loop has no jobs, waiting")
            (Thread/sleep 30000))))
      (catch org.sqlite.SQLiteException e
                                        ; probably a lock held
        (error "sqlite exception " e)
        (if (clojure.string/includes? (.getMessage e) "SQLITE_BUSY")
          (do
            (debug "SQLITE BUSY, waiting")
            (Thread/sleep 1000))
          (throw+)))))))


(defn main-loop-match-to-summ
  [db]
  (let [transducer (match-to-summ/make-match-to-summoner-transducer db)
        job-table "match_job_queue"]
    (while true
      (try+
      (app-db/update-job-queue-for-timed-out db job-table)
      (app-db/close-failed-jobs db job-table)
       (let [job-id (app-db/get-job-from-job-queue db job-table)]
         (if (not (nil? job-id))
           (try+
            (transduce transducer
                       conj
                       [(match-to-summ/build-match-to-summoner-job-map db job-id)])
            (catch [:status 403] {:keys [request-time headers body]}
              (throw+))
            (catch [:status 404] {:keys [request-time headers body]}
              (error "404 during match-to-summ job, failing on table " job-table " id " job-id)
               (app-db/fail-job db job-table job-id))
            (catch [:status 400] {:keys [request-time headers body]}
              (error "400 during match-to-summ job, failing on table " job-table " id " job-id)
              (app-db/fail-job db job-table job-id))
            (catch [:status 503] {:keys [request-time headers body]}
              (error "503, waiting")
              ;(app-db/fail-job db job-table job-id)
              (Thread/sleep 5000))
            (catch [:status 504] {:keys [request-time headers body]}
              (error "504, waiting")
              ;(app-db/fail-job db job-table job-id)
              (Thread/sleep 5000))
            (catch Object e
              (throw+)))
           (do
             (info "match-to-summ loop has no jobs, waiting")
             (Thread/sleep 30000))))
       (catch org.sqlite.SQLiteException e
                                        ; probably a lock held
         (error "sqlite exception " e)
         (if (clojure.string/includes? (.getMessage e) "SQLITE_BUSY")
           (do
             (debug "SQLITE BUSY, waiting")
             (Thread/sleep 1000))
           (throw+)))))))


(defn -main
  [& args]
  (let [c1 (async/thread (main-loop-summ-to-mmr db))
        c2 (async/thread (main-loop-mmr-to-match db))
        c3 (async/thread (main-loop-match-to-summ db))]
    (async/<!! c1)
    (async/<!! c2)
    (async/<!! c3)))
