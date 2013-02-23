(ns ecomp2.feat2)
(use '(incanter core io))

(def ymin [:yes :maybe :invited :no])
(defn number-of-friends [{:keys [user-id user_friends_csv] :as m}]
  "count of friends of user-id"
  (count ($ :friends ($where {:user user-id} user_friends_csv))))

(defn number-of-friends-going-yes-maybe-invited-no [{:keys [user-id event-id 
                                                            event_attendees_csv user_friends_csv] :as m}]
  "number of friends who have marked yes,maybe,invited,no"  
  (let [all ($where {:event event-id} event_attendees_csv)
        fnx (fn [k] (let [res ($ k all)] (if (sequential? res) (set res) (hash-set res))))
        all-friends (set ($ :friends ($where {:user user-id} user_friends_csv)))
        fncount (fn [k] (count (clojure.set/intersection (fnx k) all-friends)))]
    (zipmap ymin (map fncount ymin))))

(defn percent-of-friends-going [{:keys [user-id event-id] :as m} ]
  "input is user-id event-id pair 
How to get this? - Find out total friends, number of friends going "
  (let [no-of-friends (number-of-friends m)]
    (if (> no-of-friends 0)
      (let [no-of-friend-going (number-of-friends-going-yes-maybe-invited-no m)]
        (zipmap ymin (map #(float (/ (no-of-friend-going %1) no-of-friends)) ymin)))
      (zipmap ymin [0.0 0.0 0.0 0.0]))))

(defn user-joined-before-event-started? [{:keys [user-id event-id events_csv users_csv] :as m}]
  "return 1 if user joined before event started"
  (let [user-joined-time ($ :joinedAt ($where {:user_id user-id} users_csv))
        event-start-time ($ :start_time ($where {:event_id event-id} events_csv))]
    (assoc {} :joined-before-started (if (.before user-joined-time event-start-time) 1 0))))

(defn user-viewed-event-start-date-diff [{:keys [user-id event-id events_csv train_csv] :as m}]
  "return number of days between event start and user knowing about event"
  (let [user-viewed-time (.getTime ($ :timestamp ($where {:user user-id :event event-id} train_csv)))
        edate ($ :start_time ($where {:event_id event-id} events_csv))]
    (assoc {} :viewed-start-date-diff
           (if (instance? java.util.Date edate)
             (float (/ (- (.getTime edate) user-viewed-time) (* (* 1000 60) 60))) 
             -1))))

(defn place-features [{:keys [location city state country ] :as m}]
  (do
    ;(println (str "m  "  location "city " city "state" state "country" country))
                  (let [tokens (vec (.split location " "))
        fnx (fn [x] (do ;(println (str "tokens " tokens "x " x)) 
                      (if (or (nil? x) (sequential? x) (= 0 (.length x))) 0 
                        (if (empty? (filter 
                                  #(.equalsIgnoreCase x %) tokens)) 0 1))))]
    (zipmap [:city :state :country ] (map fnx [city state country ])))))

(defn get-place-features [{:keys [user-id event-id events_csv users_csv] :as m}]
    (let [user-location ($ :location ($where {:user_id user-id} users_csv))
        event-coords (to-map ($where {:event_id event-id} events_csv))]
      ;(println (str "location "   (vec  event-coords)))
      (place-features (assoc event-coords :location user-location))))

(defn get-popularity [{:keys [event-id event_attendees_csv ] :as m}]
  (assoc {} :popularity (count ($ :yes ($where {:event event-id} event_attendees_csv)))))
    
(defn generate-csv [{:keys [train_csv] :as m}]
  (let 
    [dset (to-dataset 
            (for [i (:rows train_csv)] 
              (let [pofg-input (assoc m :user-id (i :user) :event-id (i :event))]
                (println (str " ith item " (i :user) " event " (i :event)))
                (reduce into {:user-id (i :user) :event-id (i :event) 
                       :invited (i :invited) 
                       :interested (i :interested)}
                        (map #(% pofg-input) 
                             [percent-of-friends-going get-place-features
                              get-popularity user-viewed-event-start-date-diff])))))]
    (save dset "/home/kiran/kaggle/eventcomp/data/testop.csv")))
    