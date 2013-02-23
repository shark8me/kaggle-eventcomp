(ns ecomp2.feat1test)
(use '[ecomp2.feat2 :as f])
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])
(use '[clojure.test :as t])
(use '(incanter core io))



(defn minput_create [user-friend ev_att trai usr evts]
  (let [user-friends (lf/load-user_friends_csv user-friend)
        event-attendees (lf/load-event_attendees_csv ev_att)
        train_csv (lf/load-train_csv trai)
        users_small (lf/load-users_csv usr)
        events_small (lf/load-events_csv evts)]
    (assoc {}  :user_friends_csv user-friends
                      :event_attendees_csv event-attendees
                      :users_csv users_small
                      :train_csv train_csv
                      :events_csv events_small)))

(def minput_small (minput_create d/ufsmall d/easmall d/trainsmall d/userssmall d/eventssmall))
;(def minput (minput_create uf ea train users events))
    
(t/deftest test1
  "test count of number of friends of user-id"
  (t/is (= 382 (f/number-of-friends (assoc minput_small :user-id 2966052962))))
    (t/is (= 0 ((f/number-of-friends-going-yes-maybe-invited-no (assoc minput_small :user-id 2966052962
                                                           :event-id 2688888297)) :yes)))
    ;5574997,1423412400
    ;5574997,1916133216
    (t/is (= {:no 0.0, :invited 0.0, :maybe 0.0, :yes 0.0}
             (f/percent-of-friends-going (assoc minput_small :user-id 5574997
                                                :event-id 1916133216))))
    ;(t/is (nil? (f/percent-of-friends-going-seq minput_small)))
    (t/is (= {:joined-before-started 1} (f/user-joined-before-event-started? 
                 (assoc minput_small :user-id 2752000443 
                        :event-id 684921758))))
    (t/is (= {:joined-before-started 0} (f/user-joined-before-event-started? 
                 (assoc minput_small :user-id 627175141 
                        :event-id 684921758))))
    (t/is (= 573 (int ((f/user-viewed-event-start-date-diff 
                 (assoc minput_small :user-id 3044012 
                        :event-id 2529072432)) :viewed-start-date-diff))))
    (t/is (= {:country 1, :state 0, :city 1}
             (f/get-place-features 
                 (assoc minput_small :user-id 3197468391 
                        :event-id 2529072432))))
    (t/is (= {:popularity 7}
             (f/get-popularity 
                 (assoc minput_small :event-id 1159822043))))
    )

(test1)
(comment 
  (try
    (f/generate-csv minput)      
    (catch Throwable t
      (.printStackTrace t))))



