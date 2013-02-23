(ns ecomp2.loadfilestest)
(use '[ecomp2.loadfiles :as lf])
(use '[clojure.test :as t])

(t/deftest test2
  "test count of number of friends of user-id"
  (let [user-friends (lf/load-user_friends_csv 
                       "/home/kiran/kaggle/eventcomp/data/user_friends_small.csv")
        event-attendees (lf/load-event_attendees_csv
                       "/home/kiran/kaggle/eventcomp/data/event_attendees_small.csv")]
    (t/is (= 9 (count user-friends)))
    (t/is (= 9  (count event-attendees)))))

(t/deftest test3
  "test loading of train.csv"
  (let [train_csv (lf/load-train_csv 
                       "/home/kiran/kaggle/eventcomp/data/train_small.csv")]
    (t/is (= '(3044012 3044012) (map :user-id (take 2 train_csv))))
    ))


