(ns ecomp2.loaddstest)
(use '[clojure.test :as t])
(use '[ecomp2.loadds :as ld])
(use '(incanter core io))

(t/deftest test2
  "test count of number of friends of user-id"
  (let [user-friends (ld/load-user_friends_csv 
                       "/home/kiran/kaggle/eventcomp/data/user_friends_small.csv")]
    (t/is (= 9 (count ($ :user user-friends))))))

(t/deftest event-attend
  "test count of event attendees"
  (let [event-attendees (ld/load-event_attendees_csv
                          "/home/kiran/kaggle/eventcomp/data/event_attendees_small.csv")]
    (t/is (= 9 (count ($ :event event-attendees))))    ))

(t/deftest train-load
  "test loading of train_csv"
  (let [train_csv (ld/load-train_csv
                          "/home/kiran/kaggle/eventcomp/data/train_small.csv")]
    (t/is (= 9 (count ($ :user train_csv))))))

(t/deftest events-load
  "test loading of events_csv"
  (let [e_csv (ld/load-events_csv
                          "/home/kiran/kaggle/eventcomp/data/events_small.csv")]
    (t/is (= 9 (count ($ :user_id e_csv))))))

(t/deftest users-load
  "test loading of users_csv"
  (let [u_csv (ld/load-users_csv
                          "/home/kiran/kaggle/eventcomp/data/users_small.csv")]
    (t/is (= 9 (count ($ :user_id u_csv))))))


