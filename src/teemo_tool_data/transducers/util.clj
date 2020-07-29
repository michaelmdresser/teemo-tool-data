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
  (trace "finishing job on table " queue-table " for job id " job-id)
  (app-db/finish-job-from-queue db queue-table job-id)
  (debug "finished job on table " queue-table " for job id " job-id)
  job-id)

; dropall is a transducer that drops everything
(def dropall (drop-while (fn [item] true)))
