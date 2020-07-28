(ns teemo-tool-data.transducers.mmr-to-match
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


(defn normal-mmr-under?
  ; {:job-id ? :mmr-json ? :summoner-json ? :region ?}
  [threshold data]
  (as-> data v
    (get v :mmr-json)
    (json/read-str v)
    (get v "normal")
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
  (comp
   (filter (partial normal-mmr-under? 600))
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

