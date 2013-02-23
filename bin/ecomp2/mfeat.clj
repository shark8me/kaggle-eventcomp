(ns ecomp2.mfeat)
(use '(incanter core io mongodb))
(use 'somnium.congomongo)
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])

;each method in this file gets one feature out from the dataset.
;this implementation directly queries the db rather than load the dataset in memory.

(def ymin [:yes :maybe :invited :no])

(defn parseT [parsefn toparse]
  (if (instance? java.util.Date toparse)
    toparse (parsefn toparse)))

;the parse function in users_csv and events_csv
(def parseV1 (partial parseT lf/parse-time))

(defn number-of-friends [{:keys [user-id] :as m}]
  "count of friends of user-id"
  (let [f1 (first  (fetch :user_friends_csv 
          :where {:user user-id} :only [:friends] ))]
    (try
      (if (nil? f1) 0
        (let [cntOn (f1 :friends)]
          ;(println (str "in no-of-friends " cntOn))
          (if (instance? Long cntOn) 1 
          (count cntOn))))
      (catch Throwable t 
        (println (str " no of friends " (.getMessage t)))))))

(defn number-of-friends-going-yes-maybe-invited-no [{:keys [user-id event-id ] :as m}]
  "number of friends who have marked yes,maybe,invited,no"  
  (let [all (first (fetch :event_attendees_csv :where {:event event-id}))
        fnx (fn [k] (let [res (all k)] (if (sequential? res) (set res) (hash-set res))))
        all-friends (let [setinput (:friends (first (fetch :user_friends_csv
                         :where {:user user-id} :only [:friends])))]
                      ;(println (str "type of setinput " (type setinput)))
                      (if (sequential? setinput) (set  setinput) (hash-set setinput))) 
        fncount (fn [k] 
                  (let [r (clojure.set/intersection (fnx k) all-friends)]
                    ;(println (str "in no-of-friends-yes-maybe " (fnx k)))
                     (count r)))]
    (zipmap ecomp2.mfeat/ymin (map fncount ecomp2.mfeat/ymin))))

(defn percent-of-friends-going [{:keys [user-id event-id] :as m} ]
  "input is user-id event-id pair 
How to get this? - Find out total friends, number of friends going "
  (zipmap ymin
          (let [no-of-friends (number-of-friends m)]
            (if (> no-of-friends 0)
              (let [no-of-friend-going (number-of-friends-going-yes-maybe-invited-no m)]
                (map #(float (/ (no-of-friend-going %1) no-of-friends)) ymin))
              [0.0 0.0 0.0 0.0]))))

(defn user-joined-before-event-started? [{:keys [user-id event-id] :as m}]
  "return 1 if user joined before event started"
  (let [user-id 2752000443
      event-id 684921758
      f1 (first (fetch :users_csv :where {:user_id user-id}))
        f2 (first (fetch :events_csv :where {:event_id event-id} ))]
    ;(println (str " f1 " (type (f1 :joinedAt )) " f2 " (type  (f2 :start_time)) ))
    (assoc {} :joined-before-started 
           (if (or (nil? f1) (nil? f2)) 0
             (let [ut (f1 :joinedAt)
                   st (f2 :start_time)
                   user-joined-time (parseV1 ut)                                      
                   event-start-time  (parseV1 st)]
               (if (.before user-joined-time event-start-time) 1 0))))))


(defn user-viewed-event-start-date-diff [{:keys [user-id event-id timestamp] :as m}]
  "return number of days between event start and user knowing about event"
  (let [ user-viewed-time timestamp
        ;(:timestamp (first (fetch :train_csv :where {:user user-id :event event-id})))
      edate (:start_time (first (fetch :events_csv :where {:event_id event-id})))]
  (assoc {} :viewed-start-date-diff
         (if (or (nil? user-viewed-time) (nil? edate)) -1
           (let [ut (parseV1 user-viewed-time)
                 et (parseV1 edate)]
             (float (/ (- (.getTime et) (.getTime ut)) (* (* 1000 60) 60))))))))

(defn place-features [{:keys [location city state country ] :as m}]
  (do
    ;(println (str "m  "  location "city " city "state" state "country" country))
                  (let [tokens (vec (.split location " "))
        fnx (fn [x] (do ;(println (str "tokens " tokens "x " x)) 
                      (if (or (nil? x) (sequential? x) (= 0 (.length x))) 0 
                        (if (empty? (filter 
                                  #(.equalsIgnoreCase x %) tokens)) 0 1))))]
    (zipmap [:city :state :country ] (map fnx [city state country ])))))

(defn get-place-features [{:keys [user-id event-id ] :as m}]
  "returns a map that has a 1/0 indicating if user's location matches event's location,
     where location is city,town,country"
    (let [user-location (:location (first (fetch :users_csv :where {:user_id user-id})))
        event-coords  (first (fetch :events_csv :where {:event_id event-id} ))]
      ;(println (str "location "   (vec  event-coords)))
      (place-features (assoc event-coords :location user-location))))

(defn get-event-features [{:keys [user-id event-id ] :as m}]
  "returns a map that has all the event features"
    (let [ eventmap  (first (fetch :events_csv :where {:event_id event-id} ))]
      ;(println (str "location "   (vec  event-coords)))
      (dissoc  eventmap :_id :lat :lng :country :event_id :start_time
               :user_id :city :zip :state)))

(defn get-user-features [{:keys [user-id  ] :as m}]
  "get user features such as age & gender"
  (let [ue  (first (fetch :users_csv :where {:user_id user-id} ))
        age (let [a1 (:birthyear ue) ]
              (if (instance? Long a1) (- 2012 a1)  24))
        gender (if (.equalsIgnoreCase "male" (:gender ue)) 1 0)]
    {:age age :gender gender}))

(defn get-friend-features []
  (let [all-users (into
            (set (map :user  (fetch :train_csv)))
            (set (map :user  (fetch :test_csv))))]
    (mass-insert! :all-users (map #(assoc {} :u %) all-users))
  ))

(defn get-unique-friend-list []
  (count (reduce into #{}
          (for [i(fetch :all-users :only [:u])]
            (let [frnds (:friends (first (fetch :user_friends_csv
                                         :where {:user (:u i)}
                                         :only [:friends])))]
              (if (instance? Long frnds) (hash-set frnds) (set frnds)))))))

(defn get-popularity [{:keys [event-id  ] :as m}]
  "returns a count of the number of users who said yes to an attending an event"
  (let [res (:yes (first (fetch :event_attendees_csv :where {:event event-id} )))]
    { :popularity (if (sequential? res) (count res) 1)}))

(defn generate-csv2 [ features from-dataset destn-dataset featfn]
  (let [fnx (fn [{:keys [user event invited ] :as i}]
              (try
                (let [amap {:user-id user :event-id event}
                      rowin (into i amap)
                      currow (reduce into (into amap (featfn i)) (pmap #(% rowin) features))]
                  ;return count to prevent printing each dataset to console
                  (count (insert-dataset destn-dataset (to-dataset currow))))
              (catch Throwable t
                (println (str "caught exception on " {:user-id user :event-id event} ))
                (.printStackTrace t))))]
    (apply + (pmap fnx (fetch from-dataset)))))

(defn generate-train-csv [features]
  (generate-csv2 features :train_csv :results-train
                 (fn [i] {:invited (i :invited) :interested (i :interested)})))

(defn generate-test-csv [features]
  (generate-csv2 features :test_csv :results-test
                 (fn [i] {:invited (i :invited)})))

;(generate-test-csv)

(defn prepare-input [fnx outfile in-dataset ]
  "output the data from in-dataset to outfile in the format 
   defined by fnx"
  (let [inp (fn [i] (dissoc i :event-id :user-id :_id))
        res (clojure.string/join "\n" 
                                 (map #(fnx (inp %)) (fetch in-dataset)))]
    (spit outfile res)))

(defn prepare-svm-input [outfile in-dataset ]
  "output the data from in-dataset to outfile in the format required by libsvm"
  (prepare-input 
    (fn [inp] (let [dform (new java.text.DecimalFormat "0.000000")
                    dformat (fn [x] (-> dform (.format x)))]
                (str (if-let [a (inp :interested)] a (rand-int 2)) " " 
                     (clojure.string/join " "
                                          (map #(str %1 ":" (dformat %2)) (range 1 116)
                                               (vals (dissoc inp :interested)))))))
    outfile in-dataset))

(defn prepare-octave-input [outfile in-dataset ]
  "output the data from in-dataset to outfile in the format required by libsvm"
  (prepare-input 
    (fn [inp] (let [dform (new java.text.DecimalFormat "0.000000")
                    dformat (fn [x] (-> dform (.format x)))]
                (str (if-let [a (inp :interested)] a (rand-int 2)) "," 
                     (clojure.string/join "," (map dformat
                                                   (vals (dissoc inp :interested)))))))
    outfile in-dataset))

(defn prepare-feature-csv 
  "query the db and prepare the feature file ready for classification"
  ([filename ] (prepare-feature-csv filename true))
  ([filename istrain]
    (let [ col-to-select [:yes :no :maybe :country :state :city
                          :invited :popularity
                          :viewed-start-date-diff]
          g1 (to-dataset (fetch (if (true? istrain)
                                  :results :testset) :only 
                                (if (true? istrain)
                                  (conj col-to-select :interested)
                                  col-to-select)))
          within24h (fn [x] (do ;(println (str "x val " x)) 
                              (if (> x 24.0) 1 0)))
          divbymax (fn [x] (float (/ x 10000)))
          ]
      (save
        (ecomp2.loadds/tfrm g1 [[:popularity divbymax ] [:viewed-start-date-diff within24h]])
        filename)
      )))

;(prepare-feature-csv "/home/kiran/kaggle/eventcomp/data/testlarge.csv" false)

(def mycols [:age :popularity :viewed-start-date-diff ])
(def mycols1 (into mycols [:dim1 :dim2 :dim3 :dim4 :dim5 :dim6]))
(def mycols_all 
  (into mycols1 (conj (for [ i (range 1 101)]
                       (keyword (str "c_" i))) :c_other)))

(defn get-aggregates [mycols mydataset]
  (let [kfn (fn [pref] (map keyword (map #(str pref %) mycols)))
        vfn (fn [pfn] (map #(assoc {} pfn %) 
                           (map #(str "$" (second (.split (str %) ":"))) 
                                mycols)))
        amap (into (zipmap (kfn "max") (vfn :$max))
                   (zipmap (kfn "min") (vfn :$min)))]
    (aggregate
      mydataset
      {:$group (into  {:_id "$none" } amap)})))


(comment
(get-aggregates mycols_all :results-train))

(defn scale1 [{:keys [toscale max min] :as i}]
  (let [res (float (/  (- toscale min) (- max min)))]
    (if (< res 0) (println (str " negative val " res " toscale " toscale 
                                "max " max " min " min)))
    res)
  )

(defn scale2 [kname resmap minmaxmap]
  (assoc resmap kname 
   (scale1 {:toscale (kname resmap)
    :max (minmaxmap (keyword (str "max" kname)))
    :min (minmaxmap (keyword  (str "min" kname)))})))

;(scale1 73 10000 0)

(comment 
(scale2 :popularity
        (fetch-one :results-train)
        {:_id nil, :max:viewed-start-date-diff 18019.77734375, 
         :max:popularity 10000, 
         :max:age 107, :min:viewed-start-date-diff -605.9580688476562, 
         :min:popularity 0, :min:age 13}))

(defn f-scale [from-collname to-collname forcols]
  "scales the documents in from-collname and inserts them in to-collname
   only cols in mycols are chosen"
  (let [minmaxmap (first (:result (get-aggregates forcols from-collname))) ]
    (count (for [i  (fetch from-collname)]
             (insert! to-collname  
                      (reduce #(scale2 %2 %1 minmaxmap) i forcols))))))

(comment
(try
(f-scale :results-train)
(catch Throwable t
  (.printStackTrace t))))



(defn prepare-test-feature-csv [filename]
  "query the db and prepare the test feature file ready for classification"
    (let [ col-to-select [:yes :no :maybe :country :state :city
                          :invited :popularity
                          :viewed-start-date-diff]
          from_testcsv   (second  (second   ($ [:user :event] 
                                               (read-dataset 
                                  "/home/kiran/kaggle/eventcomp/data/test.csv"
                                  :header true))))
          testsetorder (for [{:keys [event user] :as m} from_testcsv ]
                         (dissoc (first (fetch :testset :only col-to-select
                                :where {:event-id event :user-id user})) :_id))
          g1 (to-dataset testsetorder)
          within24h (fn [x] (do ;(println (str "x val " x)) 
                              (if (> x 24.0) 1 0)))
          divbymax (fn [x] (float (/ x 10000)))
          ]
      (save
        (ecomp2.loadds/tfrm g1 [[:popularity divbymax ] [:viewed-start-date-diff within24h]])
        filename)))

;(prepare-test-feature-csv "/home/kiran/kaggle/eventcomp/data/testlarge.csv" )

(defn add-predicted-col [from-coll new-coll predfile]
  "adds the predicted value to the collection and creates a new collection"
  (count (map 
           #(update! new-coll %1 (assoc %1 :pred %2))    
           (fetch from-coll)  ($ :pred (read-dataset 
                                         predfile :header true)))))

(defn add-matfactor-pred [coll new-coll in-dataset]
  ;add the matrix factorization column to the coll collection.
  (count (for [{:keys [pred user event] :as m}   (second (second in-dataset))]
    (let [old1 (first (fetch coll :where {:user-id user, :event-id event}
                             :only [:user-id :event-id :pred :popularity]))  
          nentry (assoc (dissoc old1 :_id) :mfactorpred pred)]
      ;(println (str " ncol " nentry " m " m " old " old1))
      (insert! new-coll nentry)))))

(defn get-order-of-users-in-testcsv []
  (reduce (fn [x y] (if (= y (last x)) x 
                    (conj x y))) []
          ($ :user (read-dataset 
                     "/home/kiran/kaggle/eventcomp/data/test.csv"
                     :header true))))

(defn get-sorted-eventids [user-id]
  (mapv :event-id 
       (reverse (sort-by #(:pred %)
                         (map #(dissoc % :_id) 
                              (take 15 (fetch 
                                         :logistic-res :where {:user-id user-id}
                                         :only [:event-id :pred])))))))
(defn generate-submit-file [outfile]
  (let [userorder (get-order-of-users-in-testcsv)]
    (spit 
      outfile
      (clojure.string/join "\n"
                           (for [i userorder] 
                             (str i ",\"[" (clojure.string/join "L," 
                                                                (get-sorted-eventids i)) "]\""))))))

(defn submission [filein fileout]
  (let [ l1 (map #(vector (:user-id %) 
              (:event-id %))
                 (fetch :testset :only 
                        [:user-id :event-id] ))
        pred_in (slurp filein)
        pred_in1 (mapv #(Integer/parseInt %) (.split pred_in "\n"))
        f2 (mapv #(conj %1 %2) l1 pred_in1)
        inp  f2
        map1 (reduce #(assoc %1 (first %2) []) {}  inp)
        userorder (rest (reduce 
                    (fn [x y] (if (= y (last x)) x
                                (conj x y))) [0] 
                    ($ :user (read-dataset 
                               "/home/kiran/kaggle/eventcomp/data/test.csv"
                               :header true))))         
        finmap (reduce (fn [min [u e i]]
                         (assoc min u (conj (min u) [i e])))
                       map1 inp)
        lineentry (fn [k ] (str k ",\"[" (clojure.string/join "," (mapv #(str (last %) "L") 
                                   (reverse (sort-by
                                              first   (finmap k))))) "]\""))
        strres (clojure.string/join "\n" (for [ i userorder] 
                 (lineentry i)))]
  (spit fileout strres)))


;(submission
;  "/home/kiran/kaggle/eventcomp/data/test_svm_out_atmp2.csv"
;  "/home/kiran/kaggle/eventcomp/data/submit.csv")

(defn insert-event-word-columns []
  "inserts columns into the results collection. These cols 
   are the porter stemmed word descriptions of the event
   in the result row"
  (let [plainfetch (fetch :results)
         event-ids (map :event-id 
                plainfetch)
         cnames (conj (mapv #(keyword (str "c_" %)) 
                            (range 1 101)) :c_other)
         act_event (fn [x] 
                     (dissoc (first (fetch :events_csv
                          :where {:event_id x}
                          :only cnames)) :_id))
         to_insert (map act_event event-ids)
         ]
     (map #(update! :results %1 
                    (into %1 %2)) plainfetch to_insert)))

;(insert-event-word-columns)
;inserts unequal number of columns!!