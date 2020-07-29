(ns teemo-tool-data.transducers.summoner-to-mmr
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

(defn predicate-recently-updated?
  ; {:summoner-json ? :region ? :job-id ?}
  [db data]
  (let [summoner-data (json/read-str (get data :summoner-json))
        region (get data :region)
        account-id (get summoner-data "accountId")
        query-response (sql/query db ["SELECT COUNT(*) FROM mmr
WHERE JSON_EXTRACT(summoner_json, '$.accountId') = ?
AND   timestamp > DATETIME(CURRENT_TIMESTAMP, '-7 days')" account-id])
        count-recent (get (first query-response) (keyword "count(*)"))
        recently-updated (> count-recent 0)]
    (when recently-updated
      (app-db/finish-job-from-queue db
                                    "summoner_data_job_queue"
                                    (get data :job-id)))
    recently-updated))

(defn enrich-summoner-data-with-mmr
  ; {:summoner-json ? :region ? :job-id ?}
  ; will enrich but have to deal with nils (from 404)
  [data]
  (trace "starting enrich summoner data with mmr for data " data)
  (as-> data v
    (get v :summoner-json)
    (json/read-str v)
    (get v "name")
    (mmr/get-mmr-json-for-summoner v (get data :region))
    (assoc data :mmr-json v)
  ))


(defn mmr-is-nil?
  [data]
  (trace "in mmr-is-nil, data is " data)
  (= (get data :mmr-json) nil))


(defn insert-summoner-data-mmr-step
  ; {:summoner-json ? :region ? :job-id ? :mmr-json ?}
  [db {summoner-json :summoner-json
       region :region
       job-id :job-id
       mmr-json :mmr-json}]
  (trace "inserting mmr data for job id " job-id)
  (let [insert-response (sql/insert! db
                                     :mmr
                                     {:summoner_json summoner-json
                                      :region region
                                      :mmr_json mmr-json
                                      :type 0 ; corresponds to current mmr provider
                                      })
        mmr-id (get (first insert-response) (keyword "last_insert_rowid()"))]
    (trace "adding mmr job for mmr id " mmr-id)
    (app-db/add-job-to-mmr-queue db mmr-id)
    job-id))

(defn make-summoner-mmr-transducer
  [db]
  ; {:summoner-json ? :region ? :job-id ?}
  (comp
   (filter #(not (predicate-recently-updated? db %)))
   (map enrich-summoner-data-with-mmr)
   (filter #(not (mmr-is-nil? %)))
   (map (partial insert-summoner-data-mmr-step db))
   (map (partial mark-done-step db "summoner_data_job_queue"))
   dropall))

;(def summoner-mmr-channel (async/chan 1 (make-summoner-mmr-transducer db)))

;(def my-summoner-json (riot/get-summoner-json-from-summoner-name "eternal delta" "na1"))

;(app-db/add-job-to-summoner-queue db my-summoner-json)

;(async/>!! summoner-mmr-channel {:summoner-json my-summoner-json :region "na1" :job-id 16})

;(async/<!! summoner-mmr-channel)


(defn get-and-start-summoner-to-mmr-job
  [db cn]
  (app-db/update-job-queue-for-timed-out db "summoner_data_job_queue")
  (let [job-id (app-db/get-job-from-job-queue db "summoner_data_job_queue")
        query-response (sql/query db ["SELECT *
                                       FROM summoner_data_job_queue
                                       WHERE id = ?" job-id])
        query-row (first query-response)]
    (if (not (nil? job-id))
    (async/>!! cn {:job-id job-id
                   :region (get query-row :region)
                   :summoner-json (get query-row :summoner_json)})
    :errnojob)))

(defn manual-create-summoner-job-by-summoner-name
  [db summoner-name region]
  (let [summoner-json (riot/get-summoner-json-from-summoner-name summoner-name region)]
    (app-db/add-job-to-summoner-queue db summoner-json region)))
