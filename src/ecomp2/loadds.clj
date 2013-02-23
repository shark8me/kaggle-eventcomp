(ns ecomp2.loadds)
(use '(incanter core io))
(use '[ecomp2.files :as d])
(defn maplong [s] (do 
                    ;(println (str "maplong " s ))
                    (if (instance? java.lang.String s) 
                      (if (= 0 (.length s)) '()
                        (map #(Long/parseLong %) (.split s " ")))
                      s)))
(def eventstimeformat1 "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
(def traintimeformat1 "yyyy-MM-dd' 'HH:mm:ss.SSSSSS'+'00':'00")
(def traintimeformat2 "yyyy-MM-dd' 'HH:mm:ss'+'00':'00")
(def testtimeformat2 "yyyy-MM-dd' 'HH:mm:ss.SSS000'+'00':'00")
(defn parse-time-base [ timeformat t1]
  ;(doto (new java.text.SimpleDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 
    (try
      (-> (new java.text.SimpleDateFormat timeformat) 
        (.parse (.trim t1))) 
           (catch Throwable pe
             (do (println (str "caught exception parsing date " t1 ))
               (try
                 (-> (new java.text.SimpleDateFormat traintimeformat2) 
                   (.parse (.trim t1)))
                 (catch Throwable t1
                   (do
                     (println (str "caught exception parsing date again " t1 ))
                     (new java.util.Date))))))))
(def parse-time (partial parse-time-base eventstimeformat1))
(def parse-time-test (partial parse-time-base testtimeformat2))
(def parse-time-train (partial parse-time-base traintimeformat1))

(defn loadfile [filename colnames]
  (let [ea1 (read-dataset filename :header true)]
    (reduce #(transform-col %1 %2 ecomp2.loadds/maplong) ea1 colnames)))

(defn load-user_friends_csv [filename]
  (loadfile filename [:friends]))

(defn load-event_attendees_csv [filename]
  (loadfile filename [:yes :no :invited :maybe]))

(defn mean-normalize [dset cols ]
  (let [maxvals (for [i cols] (let [mval (vec ($ i dset) )]
                                ;(println (str "i " i "mval " mval )) 
                                (apply max mval)))
        divby (for [i maxvals]  (fn [x] (do ;(println (str "x " x "i " i)) 
                                          (if (= 0.0 x) x (float (/ x i))))))]
    (reduce (fn [r [cname cfn]] (transform-col r cname cfn)) dset 
            (map #(vector %1 %2) cols divby))))
    
(defn tfrm [dset colname-fns]
  (let [ tfn (fn [ds [cname cfn]] 
               (transform-col ds cname cfn))]
    (reduce tfn dset colname-fns)))

(defn load-train_csv [filename]
  (let [dset (read-dataset filename :header true)]
    (tfrm  dset [[:timestamp parse-time-train]])))

(defn load-test_csv [filename]
  (load-train_csv filename))

(defn load-users_csv [filename]
  (let [dset (read-dataset filename :header true)]
    (tfrm  dset [[:joinedAt parse-time]])))

(defn load-events_csv 
  "header is
   event_id,user_id,start_time,city,state,zip,country,lat,lng, c_1 to c_100, c_other"
  ([filename] (load-events_csv filename false))
  ([filename header_state] 
    (let [dset (if (true? header_state )
                 (read-dataset filename :header true)
                 (read-dataset filename))
          ;timeparsed (tfrm  dset [[:start_time parse-time]])
          cnames (conj (mapv #(keyword (str "c_" %)) (range 1 101)) :c_other)]
      (println (str "in load-events-csv " filename ))
      ;(mean-normalize (if (true? header_state)
      ;                  dset (col-names dset d/event_colnames)) cnames))))
      dset)))
      ;(if (true? header_state) dset (col-names dset d/event_colnames)))))

(comment 
(def es
  (ecomp2.loadds/load-events_csv
          "/home/kiran/kaggle/eventcomp/data/events_small.csv")))