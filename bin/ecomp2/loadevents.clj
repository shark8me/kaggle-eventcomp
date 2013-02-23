(ns ecomp2.loadevents)
(use '(incanter core io mongodb))
(use 'somnium.congomongo)
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])

 (def common_events
     (into (set ($ :event (read-dataset
                            "/home/kiran/kaggle/eventcomp/data/test.csv"
                            :header true)))
           (set ($ :event (read-dataset
                   "/home/kiran/kaggle/eventcomp/data/train.csv"
                   :header true)))))

;(count common_events)

 (def ecsv 
   (read-dataset "/home/kiran/kaggle/eventcomp/data/events_small.csv" :header true))
 
 ;(def ecsvl 
 ;  (read-dataset "/home/kiran/kaggle/eventcomp/data/events.csv" :header true))
 
 ;(time ($where {:event_id {:in (hash-set (take 5 common_events))}} ecsvl))
 
 ;(for [i (take 10 common_events)]
 ;       (insert! :events2_csv  (to-map ($where {:event_id i} ecsvl))))
 
 
(defn load-train-and-test-ids-from-events_csv [ common_events dset] 
  "header is
   event_id,user_id,start_time,city,state,zip,country,lat,lng, c_1 to c_100, c_other"
    (let [;dset (read-dataset filename :header true)
          ;timeparsed (tfrm  dset [[:start_time parse-time]])
          d 15
          ]
      (mongo! :db "largedb")
      (for [i common_events]
        (insert-dataset :results2  ($where {:event_id i} dset)))))

;(load-train-and-test-ids-from-events_csv)

