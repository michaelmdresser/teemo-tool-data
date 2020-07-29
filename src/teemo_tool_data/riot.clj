(ns teemo-tool-data.riot
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [slingshot.slingshot :refer [try+ throw+]]
   ))

(def base-api-format "https://%s.api.riotgames.com/lol/")

(defn get-line
  [filename]
  (with-open [rdr (io/reader filename)]
    (first (line-seq rdr))))

(defn get-riot-api-key
  []
  (get-line "secret/riotapikey.txt")
  )

;(get-riot-api-key)

(defn riot-get
  [querypath args & {:keys [backoffwait backoffcount]
                     :or {backoffwait 1000
                          backoffcount 0}}]
  (trace "riot-get " (get-env))
   (if (> backoffcount 6)
     (throw (Exception. "riot get max backoff retry reached")))
  (try+
   (client/get querypath args)
   (catch [:status 429] {:keys [request-time headers body]}
     (debug "got 429 for call   querypath " querypath "  headers: " headers "    body: " body "waiting: " (get headers "Retry-After"))
     (Thread/sleep (* 1000 (read-string (get headers "Retry-After"))))
     (debug "trying again")
     (riot-get querypath args :backoffwait backoffwait :backoffcount (+ backoffcount 1))
     )
   (catch [:status 504] {:keys [request-time headers body]}
     (warn "got 504 for call:  querypath " querypath "    waiting " backoffwait "ms")
     (Thread/sleep backoffwait)
     (info "backoff wait finished, retrying")
     (riot-get querypath args :backoffwait (* backoffwait 2) :backoffcount (+ backoffcount 1))
     )
   (catch [:status 404] {:keys [request-time headers body]}
     (trace "got 404 for call: " querypath args)
     (throw+)
     )
  ))

(defn get-summoner-json-from-summoner-name
  [summoner-name region]
  (trace "get-summoner-json-from-summoner-name" (get-env))
  (let [query (str (format base-api-format region) "summoner/v4/summoners/by-name/" summoner-name)]
    (get (riot-get query
                   {:headers {"X-Riot-Token" (get-riot-api-key)}
                    :accept :json})
         :body)))

(defn get-summoner-json-from-account-id
  [account-id region]
  (trace "get-summoner-json-from-account-id" (get-env))
  (let [query (str (format base-api-format region) "summoner/v4/summoners/by-account/" account-id)]
    (get (riot-get query
                   {:headers {"X-Riot-Token" (get-riot-api-key)}
                    :accept :json})
         :body)))


(defn get-account-id-from-summoner-name
  [summoner-name region]
  (trace "get-account-id-from-summoner-name" (get-env))
  (let [query (str (format base-api-format region) "summoner/v4/summoners/by-name/" summoner-name)]
    (get (json/read-str (get (riot-get query
                                         {:headers {"X-Riot-Token" (get-riot-api-key)}
                                          :accept :json})
                             :body))
         "accountId")))

; (get-account-id-from-summoner-name "eternal delta" "na1")

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

;(get-account-id-from-summoner-name "eternal delta" "na1")
;(get-summoner-name-from-account-id (get-account-id-from-summoner-name "eternal delta" "na1") "na1")

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

;(get-current-match-for-summoner "balboray" "na1")


(defn get-match-json-by-id
  [match-id region]
  (let [query (str (format base-api-format region) "match/v4/matches/" match-id)]
    (get (riot-get query
                                  {:headers {"X-Riot-Token" (get-riot-api-key)}
                                   :accept :json})
                        :body
                        )
    ))

(defn get-match-history-json-for-account-id
  [account-id region]
  (infof "getting match history for account id: %s in region: %s" account-id region)
  (let [query (str (format base-api-format region) "match/v4/matchlists/by-account/" account-id)]
    (get (riot-get query
                                  {:headers {"X-Riot-Token" (get-riot-api-key)}
                                   :accept :json})
                        :body
                        )
    ))

