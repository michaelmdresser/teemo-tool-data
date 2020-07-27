(ns teemo-tool-data.mmr
  (:require [throttler.core :refer [throttle-fn]]
            [clj-http.client :as client]
            [clojure.core.async :as async] ; necessary for throttler
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [slingshot.slingshot :refer [try+ throw+]]
            ))

(def whatismymmr-region-mapping {"na1" "na"
                                 "la1" ""})

(defn get-mmr-json-for-summoner
  [summoner-name region]
  """
Queries the whatismymmy.com API for the MMR for a provided summoner.
Throttled to a rate of 60/min to obey the request of the API provider.
The region is a region that conforms with the Riot API, this function
will map that to a valid region (if one exists).
"""
(def get-mmr-json-for-summoner-throttled
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
          ; TODO decide if throw or nil is correct here
          ;(throw+)
          nil
          )
        )
       )
     )
   60
   :minute
   ))

  (get-mmr-json-for-summoner-throttled summoner-name region))

