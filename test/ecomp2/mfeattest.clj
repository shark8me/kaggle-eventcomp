(ns ecomp2.mfeattest)
(use '(incanter core io mongodb))
(use 'somnium.congomongo)
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])
(use '[clojure.test :as t])
(use '[ecomp2.mfeat :as f])
(use '[ecomp2.glab :as glab])

;(mongo! :db "mydb")

(defn insert-all-ds []
  (do 
    (insert-dataset :user_friends_csv (lf/load-user_friends_csv d/ufsmall))
    (insert-dataset :event_attendees_csv (lf/load-event_attendees_csv d/easmall))
    (insert-dataset :users_csv (lf/load-users_csv d/userssmall))
    (insert-dataset :events_csv (lf/load-events_csv d/eventssmall))
    (insert-dataset :train_csv (lf/load-train_csv d/trainsmall))
    (insert-dataset :test_csv (lf/load-train_csv d/test_csv))
    (insert-dataset :testleftover_csv (lf/load-train_csv d/testleftover_csv))
  nil))

(defn insert_events []
  (let [fnames(into 
                (mapv #(str "x0" %) (range  1 2))
                (mapv #(str "x" %) (range 10 11)))
        hpath "/home/kiran/kaggle/eventcomp/data/ed/"
        fullpaths (mapv #(str hpath %) fnames)
        fnx (fn [i] (do ;(println (str " file loading " i)) 
                      ;(flush)
                      (insert-dataset :events_csv (lf/load-events_csv i))
                      nil))]
    (drop-coll! :events2_csv)
    (println (str " file loading start "))
    (insert-dataset :events2_csv (lf/load-events_csv (str hpath "x00") true))
    (println (str " file loading end " ))
    (println (str " file loading end aa " (mapv fnx fullpaths) ))
    nil))

;(insert_events)

(defn insert-all-large-ds []
  (do 
    (println (str "loading :user_friends_csv "
                  (count (insert-dataset :user_friends_csv (lf/load-user_friends_csv d/uf)))))
   
    (println (str "loading :event_attendees_csv " 
                  (count (insert-dataset :event_attendees_csv (lf/load-event_attendees_csv d/ea)) )))
    
    (println (str "loading :users_csv "
                  (count (insert-dataset :users_csv (lf/load-users_csv d/users)) )))
    ;some date parsing issues in users_csv-57 date fields are empty
    
    (println (str "loading :events_csv "
                  (count (insert-dataset :events_csv (lf/load-events_csv d/relevents true)))))
        
    (println (str "loading :train_csv "
                  (count (insert-dataset :train_csv (lf/load-train_csv d/train)) )))
    ;date parsing issues again-seem to be fixed.
    
    (println (str "loading :test_csv "
                  (count (insert-dataset :test_csv (lf/load-test_csv d/test_csv)) )))
    
    (println (str "loading done")))
  nil)

(comment
  (time (do 
    ;(drop-database! "seconddb")
    (mongo! :db "seconddb")    
    (insert-all-large-ds)
    ;(drop-coll! :results)
    (add-index! :event_attendees_csv  [:event])    
    (add-index! :user_friends_csv [:user])
    (add-index! :users_csv [:user_id ])
    (add-index! :events_csv [:event_id])    
    ;[percent-of-friends-going get-place-features get-popularity user-viewed-event-start-date-diff]
                          (comment f/get-event-features) 
    (let [featvector [f/percent-of-friends-going f/get-place-features
                      f/get-popularity f/user-viewed-event-start-date-diff
                      f/get-user-features]]
      (drop-coll! :results-train)
      (drop-coll! :results-test)
      
      (f/generate-train-csv [f/percent-of-friends-going f/get-place-features
                      f/get-popularity f/user-viewed-event-start-date-diff
                      f/get-user-features f/get-event-features])
      (f/f-scale :results-train-with-pca :scaled-train f/mycols_all)
      (f/generate-test-csv [f/percent-of-friends-going f/get-place-features
                      f/get-popularity f/user-viewed-event-start-date-diff
                      f/get-user-features f/get-event-features])
      (f/f-scale :results-test-with-pca :scaled-test f/mycols_all)
      ;(f/prepare-svm-input "/home/kiran/kaggle/eventcomp/data/train_input_to_svm1.csv" :scaled-train)
      ;(f/prepare-svm-input "/home/kiran/kaggle/eventcomp/data/test_input_to_svm1.csv" :scaled-test)
      (f/prepare-octave-input 
      "/home/kiran/kaggle/eventcomp/data/train_input_to_octave1.csv" :scaled-train)
      (f/prepare-octave-input 
      "/home/kiran/kaggle/eventcomp/data/test_input_to_octave1.csv" :scaled-test)
     
      (drop-coll! :logistic-res)
      (f/add-predicted-col :scaled-test :logistic-res 
                         "/home/kiran/kaggle/eventcomp/data/withpcapred.csv")
      (generate-submit-file "/home/kiran/kaggle/eventcomp/data/kaggle-submit.csv")
      
 
      (comment
        (f/submission
          "/home/kiran/kaggle/eventcomp/data/kgpred2.csv"
          "/home/kiran/kaggle/eventcomp/data/submit.csv")))
    )))

(comment 
  ;generate a file with the average of both logistic regression
  ;and matrix factorization.  
  (drop-coll! :averaged-model)
  (ecomp2.mfeat/add-matfactor-pred :logistic-res :averaged-model
                                   (glab/get-converted-dataset
                                     "/home/kiran/kaggle/eventcomp/data/pred4.csv"))
  (try
    (ecomp2.glab/get-averaged-submit :averaged-model
      "/home/kiran/kaggle/eventcomp/data/averaged_2_model_submit7.csv")
  (catch Throwable t
    (.printStackTrace t))))

(comment
  (try
    (insert-all-ds)
  (insert_events)
    ;(def d (read-dataset (str "/home/kiran/kaggle/eventcomp/data/ed/" "x00") :header true))
  (catch Throwable t
  (.printStackTrace t))))
;(insert-all-ds)
  
(t/deftest test1
  "test count of number of friends of user-id"
  (t/is (= 382 (f/number-of-friends (assoc {} :user-id 2966052962)))):user-id 3197468391 
                        :event-id 2529072432
  (t/is (= 0 ((f/number-of-friends-going-yes-maybe-invited-no (assoc {} :user-id 2966052962
                                                           :event-id 2688888297)) :yes)))
  (t/is (= {:no 0.0, :invited 0.0, :maybe 0.0, :yes 0.0}
             (f/percent-of-friends-going (assoc {} :user-id 5574997
                                                :event-id 1916133216))))
  (t/is (= {:joined-before-started 1} (f/user-joined-before-event-started? 
                                        (assoc {} :user-id 2752000443 
                                               :event-id 684921758))))
  (t/is (= 573  (int (:viewed-start-date-diff (f/user-viewed-event-start-date-diff 
                       { :user-id 3044012 
                        :event-id 2529072432})))))
  (t/is (= {:country 1, :state 0, :city 1}
             (f/get-place-features 
                 {:user-id 3197468391 
                        :event-id 2529072432})))
  (t/is (= {:popularity 7}
             (f/get-popularity 
                 {:event-id 1159822043})))
  )
;(test1)

(comment
(try
  (do
    ;(drop-database! "largedb")
    (mongo! :db "largedb")
    
    ;(insert-all-large-ds))
  (println (str "starting generate-csv "))
  (flush)
  (f/generate-test-csv))
(catch Throwable t
  (.printStackTrace t))))