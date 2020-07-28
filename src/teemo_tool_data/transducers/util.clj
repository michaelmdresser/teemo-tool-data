(ns teemo-tool-data.transducers.util
  (:require [clojure.core.async :as async]
            [teemo-tool-data.db :as app-db]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            ))

(defn mark-done-step
  [db queue-table job-id]
  (trace "finishing summoner job for job id " job-id)
  (app-db/finish-job-from-queue db queue-table job-id)
  (trace "finished summoner job for job id " job-id)
  job-id)

; dropall is a transducer that drops everything
(def dropall (drop-while (fn [item] true)))
