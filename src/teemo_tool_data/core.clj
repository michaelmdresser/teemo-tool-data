(ns teemo-tool-data.core
  (:gen-class))

(require '[clojure.java.io :as io]
         '[clj-http.client :as client]
         '[clojure.data.json :as json]
         '[clojure.java.jdbc :as sql]
         '[clojure.core.async :as async] ; necessary for throttler
         '[throttler.core :refer [throttle-fn]]
         '[taoensso.timbre :as timbre
           :refer [log  trace  debug  info  warn  error  fatal  report
                   logf tracef debugf infof warnf errorf fatalf reportf
                   spy get-env]]
         '[slingshot.slingshot :refer [try+ throw+]]
         )

(timbre/set-level! :debug)

(defn get-line
  [filename]
  (with-open [rdr (io/reader filename)]
    (first (line-seq rdr))))

(defn get-riot-api-key
  []
  (get-line "secret/riotapikey.txt")
  )

;(get-riot-api-key)

(def base-api-format "https://%s.api.riotgames.com/lol/")

;(info "test")

(defn riot-get
  [querypath args & {:keys [backoffwait backoffcount]
                     :or {backoffwait 1000
                          backoffcount 0}}]
  (debug "riot-get " (get-env))
   (if (> backoffcount 6)
     (throw (Exception. "riot get max backoff retry reached")))
  (try+
   (client/get querypath args)
   (catch [:status 429] {:keys [request-time headers body]}
     (warn "got 429 for call    "    "  headers: " headers "    body: " body "waiting: " (get headers "Retry-After"))
     (Thread/sleep (* 1000 (read-string (get headers "Retry-After"))))
     (info "trying again")
     (riot-get querypath args :backoffwait backoffwait :backoffcount (+ backoffcount 1))
     )
   (catch [:status 504] {:keys [request-time headers body]}
     (warn "got 504 for call: " "    waiting " backoffwait "ms")
     (Thread/sleep backoffwait)
     (info "backoff wait finished, retrying")
     (riot-get querypath args :backoffwait (* backoffwait 2) :backoffcount (+ backoffcount 1))
     )
   (catch [:status 404] {:keys [request-time headers body]}
     (warn "got 404 for call: " querypath args)
     (throw+)
     )
  ))

;(while true (get-account-id-from-summoner-name "eternal delta" "na1"))

(defn get-account-id-from-summoner-name
  [summoner-name region]
  (trace "get-account-id-from-summoner-name" (get-env))
  (let [query (str (format base-api-format region) "summoner/v4/summoners/by-name/" summoner-name)]
    (get (json/read-str (get (riot-get query
                                         {:headers {"X-Riot-Token" (get-riot-api-key)}
                                          :accept :json})
                             :body))
         "accountId")))

(get-account-id-from-summoner-name "eternal delta" "na1")

(defn get-summoner-name-from-account-id
  [accountid region]
  (trace "get-summoner-name-from-account-id" (get-env))
  (let [query (str (format base-api-format region) "summoner/v4/summoners/by-account/" accountid)]
    (get (json/read-str (get (riot-get query
                                         {:headers {"X-Riot-Token" (get-riot-api-key)}
                                          :accept :json})
    :body
    ))
    "name"
  )))

(defn get-summoner-id-from-account-id
  [accountid region]
  (trace "get-summoner-id-from-account-id" (get-env))
  (let [query (str (format base-api-format region) "summoner/v4/summoners/by-account/" accountid)]
    (get (json/read-str (get (riot-get query
                                         {:headers {"X-Riot-Token" (get-riot-api-key)}
                                          :accept :json})
                             :body
                             ))
         "id"
         )))

(def whatismymmr-region-mapping {"na1" "na"
                                 "la1" ""})

(def get-mmr-json-for-summoner
  (throttle-fn
(fn
  ; region is different, see the docs
  ; https://dev.whatismymmr.com/
  [summoner-name region]
  (debug "get-mmr-for-summoner" (get-env))
  (let [query (str "https://" (get whatismymmr-region-mapping (clojure.string/lower-case region)) ".whatismymmr.com/api/v1/summoner")]
    (try+
     (get
      (client/get query
                  {:accept :json
                   :query-params {"name" summoner-name}})
      :body
      )
      (catch [:status 404] {:keys [request-time headers body]}
        (warn "404 looking for summoner mmr " region " " summoner-name)
        (throw+))
    )
    )
  )
60
:minute
))


;(get-account-id-from-summoner-name "eternal delta" "na1")
;(get-summoner-name-from-account-id (get-account-id-from-summoner-name "eternal delta" "na1") "na1")

; (get-mmr-json-for-summoner "eternal delta" "na1")

(defn get-current-match-for-summoner
  [summoner-name region]
  (trace "get-current-match-for-summoner" (get-env))
  (let [accountid (get-account-id-from-summoner-name summoner-name region)
        summonerid (get-summoner-id-from-account-id accountid region)
        query (str (format base-api-format region) "spectator/v4/active-games/by-summoner/" summonerid)]
    (prn query)
    (json/read-str (get (riot-get query
                                         {:headers {"X-Riot-Token" (get-riot-api-key)}
                                          :accept :json})
                             :body
                             ))
         ))

; (get-current-match-for-summoner "balboray" "na1")

(defn get-and-insert-current-stream-match
  [db summoner-name region]
  (trace "get-and-insert-current-stream-match" (get-env))
  (let [match-data (get-current-match-for-summoner summoner-name region)
        matchid (get match-data "gameId")]
    (sql/execute! db ["INSERT INTO stream_matches (apiversion, matchId, data)
                      VALUES (?, ?, ?)
                      ON CONFLICT DO NOTHING" 4 matchid match-data])
    match-data
    ))

;(defn insert-stream-match
;  [match-data]
;  (let [matchid (get match-data "gameId")]
;    (sql/execute! db ["INSERT INTO stream_matches (matchId) VALUES (?)
;                       ON CONFLICT DO NOTHING" matchid])
;    ))

(defn get-and-insert-mmr-by-summoner-name
  [db summoner-name region]
  (trace "get-and-insert-mmr-by-summoner-name" (get-env))
  (let [mmr-data (get-mmr-json-for-summoner summoner-name region)
        account-id (get-account-id-from-summoner-name summoner-name region)]
    (if (not (= mmr-data nil))
    (sql/execute! db ["INSERT INTO mmr (accountId, region, type, data)
                      VALUES (?, ?, ?, ?)
                      " account-id region 0 mmr-data])
    mmr-data
    )
    )
  )

(defn insert-mmr-data-for-match-participants
  [db match-data is-ongoing]
  (trace "insert-mmr-data-for-match-participants" (get-env))
  (let [summoner-names (if is-ongoing
                         (map (fn [current-participant-data] (get current-participant-data "summonerName"))
                              (get match-data "participants"))
                         (map (fn [participant-identity] (get (get participant-identity "player") "summonerName"))
                              (get match-data "participantIdentities")))
        region (get match-data "platformId")]
    (debug "names" summoner-names)
  (map (fn [summoner-name]
         (try+
         (get-and-insert-mmr-by-summoner-name db
                                              summoner-name
                                              region)
             (catch [:status 404] {:keys [request-time headers body]}
               (warn "got 404 for mmr get and insert: " summoner-name region headers body)
               :error404
               )
             ))
       summoner-names)))

(defn do-stream-match-get-and-inserts
  [db summoner-name region]
  (trace "do-stream-match-get-and-inserts" (get-env))
  (let [match-data (get-and-insert-current-stream-match db summoner-name region)]
    (insert-mmr-data-for-match-participants db match-data true)))

(defn get-match-by-id
  [match-id region]
  (let [query (str (format base-api-format region) "match/v4/matches/" match-id)]
    (json/read-str (get (riot-get query
                                    {:headers {"X-Riot-Token" (get-riot-api-key)}
                                     :accept :json})
                        :body
                        ))
    ))

(defn get-match-history-for-account-id
  [account-id region]
  (infof "getting match history for account id: %s in region: %s" account-id region)
  (let [query (str (format base-api-format region) "match/v4/matchlists/by-account/" account-id)]
    (json/read-str (get (riot-get query
                                    {:headers {"X-Riot-Token" (get-riot-api-key)}
                                     :accept :json})
                        :body
                        ))
    ))

(defn get-and-insert-match
  [db match-id region]
  (trace "get-and-insert-match" (get-env))
  (let [match-data (get-match-by-id match-id region)]
    (sql/execute! db ["INSERT INTO matches (apiversion, matchId, data)
                       VALUES (?, ?, ?)
                       ON CONFLICT DO NOTHING"
                      4 match-id match-data])
    (debug "match data id" (get match-data "gameId"))
    match-data
    ))

(defn get-and-insert-match-history-for-account-id
  [db account-id region]
  (trace "get-and-insert-match-history-for-account-id" (get-env))
  (let [match-history (get-match-history-for-account-id account-id region)]
    (map (fn [match-id] (get-and-insert-match db match-id region))
         (map (fn [match-info] (get match-info "gameId"))
              (get match-history "matches"))))
  )

(defn do-account-match-history-get-and-inserts
  [db account-id region]
  (trace "do-account-match-history-get-and-inserts" (get-env))
  (let [matches-data (get-and-insert-match-history-for-account-id db account-id region)]
    (debug "GOT HERE")
    (debug matches-data)
    (debug (map (fn [match-data] (insert-mmr-data-for-match-participants db match-data false))
         matches-data))
    (debug "FINISHED INSERT MMR")))

;(get-and-insert-current-match db "tigermeup" "na1")

; (do-stream-match-get-and-inserts db "darkprinc3" "na1")

(def my-account-id (get-account-id-from-summoner-name "eternal delta" "na1"))
(do-account-match-history-get-and-inserts db my-account-id "na1")


(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "/home/delta/db/teemo-tool-data.db"})

;(clojure.pprint/pprint (get-and-insert-stream-match db "qndrew" "na1"))

(defn create-mmr-table
  [db]
  (sql/db-do-commands db "CREATE TABLE IF NOT EXISTS mmr (
                          accountId TEXT,
                          region TEXT,
                          type INTEGER,
                          data TEXT,
                          timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)"
                      ))

(create-mmr-table db)

;(defn create-accounts-table
;  [db]
;  (sql/db-do-commands db "CREATE TABLE IF NOT EXISTS accounts (
;                          accountId TEXT PRIMARY KEY,
;                          puuid TEXT
;                          )"
;                      ))
;
;(create-accounts-table db)

(defn create-matches-table
  [db]
  (sql/db-do-commands db "CREATE TABLE IF NOT EXISTS matches (
                          apiversion TEXT,
                          matchId TEXT PRIMARY KEY,
                          data TEXT
                          )"
                      ))

(create-matches-table db)

(defn create-stream-matches-table
  [db]
  (sql/db-do-commands db "CREATE TABLE IF NOT EXISTS stream_matches (
                          apiversion TEXT,
                          matchId TEXT,
                          data TEXT,
                          timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                          )"
                      ))

(create-stream-matches-table db)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
