(ns teemo-tool-data.transducers.match-to-summoner
  (:require [teemo-tool-data.db :as app-db]
            [teemo-tool-data.riot :as riot]
            [teemo-tool-data.transducers.util :refer [mark-done-step
                                                      dropall]]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [clojure.java.jdbc :as sql]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            ))

(defn get-account-info-from-match-step
  ; {:job-id ? :match-json ? :region ?}
  [data]
  (as-> data v
    (get v :match-json)
    (json/read-str v)
    (spy :trace (get v "participantIdentities"))
    (spy :trace (map (fn [x] (get x "player")) v))
    ; TODO investigate riot api weirdness with playerDTO
    (spy :trace (map (fn [x] (get x "accountId")) v))
    (spy :trace (map (fn [x] (assoc data :account-id x)) v))))

(defn get-summoner-data-for-account-step
  ; {:job-id ? :match-json ? :region ? :account-id ?}
  ; ->
  ; {:job-id ? :region ? :summoner-json ?}
  [{job-id :job-id
    region :region
    account-id :account-id}]
  (let [summoner-json (riot/get-summoner-json-from-account-id account-id region)]
    {:job-id job-id
     :region region
     :summoner-json summoner-json}))

(defn insert-summoner-data-step
                                        ; {:job-id ? :region ? :summoner-json ?}
  [db {job-id :job-id
       region :region
       summoner-json :summoner-json}]
  (trace "inserting summoner data job for job id " job-id)
  (app-db/add-job-to-summoner-queue db summoner-json region)
  job-id)


(defn make-match-to-summoner-transducer
  ; {:job-id ? :match-json ? :region ?}
  [db]
  (comp
   ; TODO mapcat here also breaks job id semantics, do I care?
   (mapcat get-account-info-from-match-step)
   (map get-summoner-data-for-account-step)
   (map (partial insert-summoner-data-step db))
   (map (partial mark-done-step db "match_job_queue"))
   dropall
   ))


(defn get-and-start-match-to-summoner-job
  [db cn]
  ; TODO update timed out jobs
  (let [job-id (app-db/get-job-from-job-queue db "match_job_queue")
        query-response (sql/query db ["SELECT match_row_id
                                 FROM match_job_queue
                                 WHERE id = ?" job-id])
        match-row-id (get (first query-response) :match_row_id)
        match-response (sql/query db ["SELECT * FROM matches
                                       WHERE rowid = ?" match-row-id])
        match-row (first match-response)
        ]
    (async/>!! cn {:job-id job-id
                   :region (get match-row :region)
                   :match-json (get match-row :match_json)})))




(comment
(app-db/create-all-tables db)

(def match-to-summoner-test-match {:riot_match_id "3509845039"
                                   :region "NA1"
                                   :match_json (riot/get-match-json-by-id "3509845039" "NA1")})

(def test-match-insert-response (sql/insert! db :matches match-to-summoner-test-match))

(def test-match-row-id (get (first test-match-insert-response) (keyword "last_insert_rowid()")))

(def test-match-job {:id 999
                     :match_row_id test-match-row-id
                     :status "READY"})

(sql/insert! db :match_job_queue test-match-job)

(def match-to-summoner-channel (async/chan 1 (make-match-to-summoner-transducer db)))


(get-and-start-match-to-summoner-job db match-to-summoner-channel)
)
