(ns teemo-tool-data.transducers.mmr-to-match
  (:require [teemo-tool-data.db :as app-db]
            [teemo-tool-data.riot :as riot]
            [teemo-tool-data.mmr :as mmr]
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


(defn normal-mmr-under?
  ; {:job-id ? :mmr-json ? :summoner-json ? :region ?}
  [threshold data]
  (trace data)
  (as-> data v
    (get v :mmr-json)
    (json/read-str v)
    (get v "normal")
    (get v "avg")
    ((fn [avg-mmr] (if (= avg-mmr nil)
                     false
                     (< avg-mmr threshold))) v)))

(defn ranked-mmr-under?
  ; {:job-id ? :mmr-json ? :summoner-json ? :region ?}
  [threshold data]
  (trace data)
  (as-> data v
    (get v :mmr-json)
    (json/read-str v)
    (get v "ranked")
    (get v "avg")
    ((fn [avg-mmr] (if (= avg-mmr nil)
                     false
                     (< avg-mmr threshold))) v)))



(defn match-history-step
  ;{ :job-id ? :mmr-json ? :summoner-json ? :region ?}
  [data]
  (as-> data v
    (get v :summoner-json)
    (json/read-str v)
    (get v "accountId")
    (riot/get-match-history-json-for-account-id v (get data :region))
    (json/read-str v)
    (get v "matches")
    (map (fn [match-reference-map]
             {:job-id (get data :job-id)
              :match-id (get match-reference-map "gameId")
              :region (get data :region)})
         v)))

(defn match-not-processed-filter
  ; {:job-id ? :match-id ? :region ?}
  [db data]
  (trace "in match not processed with data " data)
  (let [already-processed (app-db/match-already-processed? db
                                   (get data :match-id)
                                   (get data :region))]
    (when already-processed
      (app-db/finish-job-from-queue db
                                    "mmr_data_job_queue"
                                    (get data :job-id)))
    (not already-processed)))


(defn get-match-data-step
  ;{:job-id ? :match-id ? :region ?}
  [data]
  (let [match-json (riot/get-match-json-by-id (get data :match-id)
                                              (get data :region))
        ]
    (assoc data :match-json match-json)))

(defn insert-match-data-step
  [db {job-id :job-id
       match-id :match-id
       region :region
       match-json :match-json}]
  (trace "inserting match data for job id " job-id " match-id " match-id)
  (let [insert-response (sql/insert! db
                                     :matches
                                     {:riot_match_id match-id
                                      :region region
                                      :match_json match-json})
        match-row-id (get (first insert-response) (keyword "last_insert_rowid()"))]
    (trace "adding match row id " match-row-id " to match queue")
    (app-db/add-job-to-matches-queue db match-row-id)
    (trace "added match row id " match-row-id " to match queue")
    job-id))


(defn make-mmr-to-match-transducer
  [db]
  ; {:job-id ? :mmr-json ? :summoner-json ? :region ?}
  (comp
   (filter (fn [data] (or (normal-mmr-under? 650 data)
                          (ranked-mmr-under? 650 data))))
   ; mapcat turns the list of match id data into individual items in the transducer (1 summoner info -> many matches)
   ; https://stackoverflow.com/questions/59174994/how-to-create-multiple-outputs-using-a-transducer-on-a-pipeline
   (mapcat match-history-step)
   (filter (partial match-not-processed-filter db))
   (map get-match-data-step)
   (map (partial insert-match-data-step db))
   ; TODO this makes no sense because of the mapcat fan out
   (map (partial mark-done-step db "mmr_data_job_queue"))
   dropall))

;(def mmr-to-match-channel (async/chan 1 (make-mmr-to-match-transducer db)))

;(def mmr-to-match-test {:job-id 1
;                        :mmr-json "{\"normal\":{\"avg\": 200}}"
;                        :summoner-json my-summoner-json
;                        :region "na1"})

;(async/>!! mmr-to-match-channel mmr-to-match-test)

;(async/<!! mmr-to-match-channel)

(defn get-and-start-mmr-to-match-job
  [db cn]
  (app-db/update-job-queue-for-timed-out db "mmr_data_job_queue")
  (let [job-id (app-db/get-job-from-job-queue db "mmr_data_job_queue")
        query-response (sql/query db ["SELECT *
                                       FROM mmr_data_job_queue
                                       WHERE id = ?" job-id])
        query-row (first query-response)
        mmr-id (get query-row :mmr_id)
        mmr-response (sql/query db ["SELECT *
                                     FROM mmr
                                     WHERE id = ?" mmr-id])
        mmr-row (first mmr-response)]
    (if (not (nil? job-id))
    (async/>!! cn {:job-id job-id
                   :region (get mmr-row :region)
                   :summoner-json (get mmr-row :summoner_json)
                   :mmr-json (get mmr-row :mmr_json)})
    :errnojob)))

(defn manual-create-mmr-job-by-summoner-name
  [db summoner-name region]
  ; TODO maybe use the insert summoner data mmr step
  (let [summoner-json (riot/get-summoner-json-from-summoner-name summoner-name region)
        mmr-json (mmr/get-mmr-json-for-summoner summoner-name region)
        insert-response (sql/insert! db
                                     :mmr
                                     {:summoner_json summoner-json
                                      :region region
                                      :mmr_json mmr-json
                                      :type 0 ; corresponds to current mmr provider
                                      })
        mmr-id (get (first insert-response) (keyword "last_insert_rowid()"))]
    (app-db/add-job-to-mmr-queue db mmr-id)
    ))
