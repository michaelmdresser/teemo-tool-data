(ns teemo-tool-data.db
  (:require [clojure.java.jdbc :as sql]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            ))

; job queue status can be:
; READY
; INPROGRESS
; DONE
; if INPROGRESS and now > timeout, set to READY

(defn update-job-queue-for-timed-out
  [db queue-table]
  (sql/execute! db [(str "UPDATE " queue-table " SET status = ?,
                                  timeout = NULL,
                                  updated = CURRENT_TIMESTAMP
                     WHERE CURRENT_TIMESTAMP > timeout AND
                           status = ?")
                    "READY" "INPROGRESS"]))

(defn close-failed-jobs
  [db queue-table]
  (sql/execute! db [(str "UPDATE " queue-table " SET status = ?,
                                  timeout = NULL,
                                  updated = CURRENT_TIMESTAMP
                     WHERE failures >= 3")
                    "FAILED"]))

; we don't update status because it could have already been marked done by a child job due to the weird fan out behavior
(defn fail-job
  [db queue-table job-id]
  (sql/execute! db [(str "UPDATE " queue-table " SET updated = CURRENT_TIMESTAMP, failures = failures + 1
                     WHERE id = ?")
                    job-id]))

;(def db
;  {:classname   "org.sqlite.JDBC"
;   :subprotocol "sqlite"
;   :subname     "/home/delta/db/teemo-tool-data.db"})


;(update-job-queue-for-timed-out db "account_data_job_queue")

(defn set-job-inprogress
  [t-con queue-table job-id]
  (debug "setting job in progress for table " queue-table " and id " job-id)
  (sql/execute! t-con [(str "UPDATE " queue-table " SET
                         status = ?,
                         timeout = DATETIME(CURRENT_TIMESTAMP, '+10 minutes'),
                         updated = CURRENT_TIMESTAMP
                         WHERE id = ?")
                       "INPROGRESS"
                       job-id]))

(defn get-job-from-job-queue
  [db queue-table]
  (sql/with-db-transaction [t-con db]
    (let [job-ids (sql/query t-con
                   [(str "SELECT id FROM " queue-table " WHERE
                          status = ?
                          LIMIT 1")
                    "READY"])
          job-id (get (first job-ids) :id)
          ]
      (set-job-inprogress t-con queue-table job-id)
      job-id
      )))

;(get-job-from-job-queue db "account_data_job_queue")

(defn get-all-ready-jobs-from-job-queue
  ; return list of job ids
  [db queue-table]
  (sql/with-db-transaction [t-con db]
    (let [query-response (sql/query t-con
                             [(str "SELECT id FROM " queue-table " WHERE
                          status = ?")
                              "READY"])
          job-ids (map #(get % :id) query-response)
          ]
      (doall (map (fn [job-id]
                    (set-job-inprogress t-con queue-table job-id))
                  job-ids))
      job-ids
      )))

;(get-all-ready-jobs-from-job-queue teemo-tool-data.core/db "account_data_job_queue")

(defn get-n-ready-jobs-from-job-queue
                                        ; return list of job ids
  [db queue-table n]
  (sql/with-db-transaction [t-con db]
    (let [query-response (sql/query t-con
                                    [(str "SELECT id FROM " queue-table " WHERE
                          status = ? LIMIT ?")
                                     "READY" n])
          job-ids (map #(get % :id) query-response)
          ]
      (doall (map (fn [job-id]
                    (set-job-inprogress t-con queue-table job-id))
                  job-ids))
      job-ids
      )))

;(get-n-ready-jobs-from-job-queue teemo-tool-data.core/db "account_data_job_queue" 3)


(defn finish-job-from-queue
  [db queue-table job-id]
  (debug "marking job done for table " queue-table " and id " job-id)
  (sql/execute! db [(str "UPDATE " queue-table " SET
                     status = ?,
                     updated = CURRENT_TIMESTAMP,
                     timeout = NULL
                     WHERE id = ?")
                    "DONE"
                    job-id]))

;(finish-job-from-queue db "account_data_job_queue" 1)

(defn add-job-to-summoner-queue
  ; summoner-json should be a summonerdto json
  [db summoner-json region]
  (sql/insert! db
               :summoner_data_job_queue
               {:summoner_json summoner-json
                :region region
                :status "READY"}))

 ; (add-job-to-summoner-queue db "testdata123")

(defn add-job-to-mmr-queue
  ; mmr-id is an id in the mmr table
  [db mmr-id]
  (sql/insert! db
               :mmr_data_job_queue
               {:mmr_id mmr-id
                :status "READY"}))

(defn add-job-to-matches-queue
  ; match-id is a riot match id in the matches table
  [db match-row-id]
  (sql/insert! db
               :match_job_queue
               {:match_row_id match-row-id
                :status "READY"}))


(defn match-already-processed?
  [db match-id region]
  (let [query-response (sql/query db
                                  ["SELECT COUNT(*) FROM matches
                                    WHERE riot_match_id = ? AND
                                          region = ?"
                                   match-id
                                   region])
        count (get (first query-response) (keyword "count(*)"))]
    (> count 0)))

;(match-already-processed? teemo-tool-data.core/db "dummy" "na1")


(defn create-summoner-job-queue
  [db]
  (sql/db-do-commands db
                      "CREATE TABLE IF NOT EXISTS summoner_data_job_queue (
id INTEGER PRIMARY KEY,
summoner_json TEXT NOT NULL,
region TEXT NOT NULL,
status TEXT NOT NULL,
updated DATETIME DEFAULT CURRENT_TIMESTAMP,
timeout DATETIME DEFAULT NULL,
failures INTEGER NOT NULL DEFAULT 0
)"
                      ))

;(create-summoner-job-queue db)

(defn create-mmr-table
  [db]
  (sql/db-do-commands db "CREATE TABLE IF NOT EXISTS mmr (
                          id INTEGER PRIMARY KEY,
                          summoner_json TEXT NOT NULL,
                          region TEXT NOT NULL,
                          type INTEGER NOT NULL,
                          mmr_json TEXT NOT NULL,
                          timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)"
                      ))

;(create-mmr-table db)

(defn create-mmr-job-queue
  [db]
  (sql/db-do-commands db
                      "CREATE TABLE IF NOT EXISTS mmr_data_job_queue (
id INTEGER PRIMARY KEY,
mmr_id INTEGER NOT NULL,
status TEXT NOT NULL,
updated DATETIME DEFAULT CURRENT_TIMESTAMP,
timeout DATETIME DEFAULT NULL,
failures INTEGER NOT NULL DEFAULT 0,
FOREIGN KEY (mmr_id) REFERENCES mmr(id)
)"
                      ))

;(create-mmr-job-queue)

(defn create-matches-table
  [db]
  (sql/db-do-commands db
                      "CREATE TABLE IF NOT EXISTS matches (
riot_match_id TEXT NOT NULL,
region TEXT NOT NULL,
match_json TEXT NOT NULL,
PRIMARY KEY (riot_match_id, region)
)"
                      ))

; (create-matches-table db)

(defn create-match-job-queue
  [db]
  (sql/db-do-commands db
                      "CREATE TABLE IF NOT EXISTS match_job_queue (
id INTEGER PRIMARY KEY,
match_row_id INTEGER NOT NULL,
status TEXT NOT NULL,
updated DATETIME DEFAULT CURRENT_TIMESTAMP,
timeout DATETIME DEFAULT NULL,
failures INTEGER NOT NULL DEFAULT 0,
FOREIGN KEY (match_row_id) REFERENCES matches(rowid)
)"
                      ))

; (create-match-job-queue)

(defn create-stream-matches-table
  [db]
  (sql/db-do-commands db "CREATE TABLE IF NOT EXISTS stream_matches (
                          match_id TEXT NOT NULL,
                          region TEXT NOT NULL,
                          ongoing_match_json TEXT NOT NULL,
                          timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                          )"
                      ))

; (create-stream-matches-table db)


(defn create-all-tables
  [db]
  (create-summoner-job-queue db)
  (create-mmr-table db)
  (create-mmr-job-queue db)
  (create-matches-table db)
  (create-match-job-queue db)
  (create-stream-matches-table db))


