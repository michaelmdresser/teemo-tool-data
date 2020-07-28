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
            ))

(timbre/set-level! :trace)

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "/home/delta/db/teemo-tool-data.db"})

;(app-db/create-all-tables db)

(def my-account-id (riot/get-account-id-from-summoner-name "eternal delta" "na1"))

;my-account-id

;(do-account-match-history-get-and-inserts db my-account-id "na1")


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

(defn mark-done-step
  [db queue-table job-id]
  (trace "finishing summoner job for job id " job-id)
  (app-db/finish-job-from-queue db queue-table job-id)
  (trace "finished summoner job for job id " job-id)
  job-id)

; dropall is a transducer that drops everything
(def dropall (drop-while (fn [item] true)))

(defn make-summoner-mmr-transducer
  [db]
  (comp
   (map enrich-summoner-data-with-mmr)
   (filter #(not (mmr-is-nil? %)))
   (map (partial insert-summoner-data-mmr-step db))
   (map (partial mark-done-step db "summoner_data_job_queue"))
   dropall))

(def summoner-mmr-channel (async/chan 1 (make-summoner-mmr-transducer db)))

(def my-summoner-json (riot/get-summoner-json-from-summoner-name "eternal delta" "na1"))

;(app-db/add-job-to-summoner-queue db my-summoner-json)

;(async/>!! summoner-mmr-channel {:summoner-json my-summoner-json :region "na1" :job-id 16})

;(async/<!! summoner-mmr-channel)


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

(get-and-start-match-to-summoner-job db match-to-summoner-channel)


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
