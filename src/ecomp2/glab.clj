(ns ecomp2.glab)
(use '(incanter core io mongodb))
(use 'somnium.congomongo)
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])
(use '[ecomp2.mfeat :as mfeat])

;generate data for the graphlab package to parse
;generate the user-event recommendation sparse matrix, which consists
; of this tuple: user,event,reco

;reco is 1 or 0, with the remaining assumed unrated.

(comment (mongo! :db "seconddb")
         (add-index! :event_seq  [:e])
         (add-index! :event_seq  [:eid])
    (add-index! :user_seq [:u])
    (add-index! :user_seq [:uid]))

(defn eid [event]
  (:eid (first (fetch :event_seq :where {:e event}))))
(defn e-byid [eid]
  (:e (first (fetch :event_seq :where {:eid eid}))))
(defn uid [user]
  (:uid (first (fetch :user_seq :where {:u user}))))
(defn u-byid [uid]
  (:u (first (fetch :user_seq :where {:uid uid}))))

(defn lineparse [{:keys [event user interested not_interested] :as m}]
    ;(println (str " evt " event " user " user " f " ev " f " u))
  (str (eid event) "," (uid user) (if (not (nil? interested)) (str "," interested)) ))

(defn write_train_csv [outfile from-dataset]
  (spit outfile
        (clojure.string/join "\n"
                             (for [i (fetch from-dataset)]
                               (lineparse i)))))

(comment
(write_train_csv 
  "/home/kiran/kaggle/eventcomp/data/glab_input1.csv" :train_csv)
(write_train_csv 
  "/home/kiran/kaggle/eventcomp/data/glab_test_input1.csv" :test_csv))

(defn lparse [inp]
  (let [fnx (fn [in listitem itemval]
              (let [ev (eid (:event in))] ; (println (str " instance " (:event in) " " listitem " itemval " itemval ))
              (if (instance? Long listitem)
                (str ev "," (uid listitem) "," itemval)
                (clojure.string/join "\n"                        
                                     (for [i listitem]
                                       (str ev "," (uid i) "," itemval))))))]
    (clojure.string/join "\n" (mapv #(fnx inp %1 %2)
                               [(:yes inp) (:no inp)]
                               [1 0]))))

(defn write-uniq-events [infile]
  (let [eseq ($ :event
                (read-dataset infile :header true))
        numseq (iterate inc 0)]
    (count (mapv #(insert! :event_seq {:e %1 :eid %2 })
          eseq numseq))))

(defn write-uniq-users [infile]
  (let [eseq ($ :user
                (read-dataset infile :header true))
        numseq (iterate inc 0)]
    (count (mapv #(insert! :user_seq {:u %1 :uid %2 })
          eseq numseq))))

(comment
(write-uniq-events 
  "/home/kiran/kaggle/eventcomp/data/uniq_events.csv")
(write-uniq-users
  "/home/kiran/kaggle/eventcomp/data/uniq_allusers.csv"))

(defn write-ev-attendees [outfile]
  (spit outfile (clojure.string/join "\n"
    (for [i (fetch :event_attendees_csv)]
      (lparse i)))))

;(write-ev-attendees "/home/kiran/kaggle/eventcomp/data/glab_input_from_evt_attendees.csv")

;(def t1 (read-dataset
;  "/home/kiran/kaggle/eventcomp/data/glabpred1.csv" :header true))

(defn glab_submit [infile outfile]
  (let [t1 (read-dataset infile
                         :header true)
        t2 (to-dataset (for [{:keys [user event] :as m}
                             (second (second t1))]
                         (assoc m :user (u-byid user)
                                :event (e-byid event))))]
(spit outfile
(clojure.string/join "\n"
(for [i  (mfeat/get-order-of-users-in-testcsv)]  
  (str i ",\"[" (clojure.string/join "," (map :event (reverse 
                (sort-by :pred 
                         (second  (second ($ [:event :pred] ($where {:user i} t2)))))))
                                ) "]\""))))))

(defn line-sort-order [in-dataset user-id]
  "returns a list of maps, sorted by joint prediction "
(let [ flist (fetch in-dataset :where {:user-id user-id})
      fnx (fn [xfn ]
            (apply xfn (map :mfactorpred flist)))
      mmax (fnx max)
      mmin (fnx min)
      scaledfac (map (fn [{ :keys [mfactorpred pred popularity] :as m}]
                       ;sum of popularity, logistic regression, matrix factorization
                 (assoc m :joint (+ pred popularity (float 
                         (/ (- mfactorpred mmin) 
                            (let [dby (- mmax mmin) ]
                              (if (= 0 dby) 0.001 dby))))))) flist)]
  (reverse (sort-by :joint scaledfac))))

(defn get-averaged-submit [in-dataset outfile]
  "prepare a submission file that averages the score between :pred and
   :mfactorpred "
  (spit 
    outfile 
    (clojure.string/join "\n"
                           (for [i  (mfeat/get-order-of-users-in-testcsv)]  
                             (str i ",\"[" (clojure.string/join "," 
                           (map :event-id (line-sort-order in-dataset i))
                                ) "]\"")))))

(defn get-converted-dataset [infile]
  "convert the file containing predictions from glab (which contains
   event and user ids, to actual event and users"
  (let [t1 (read-dataset infile
                         :header true)
        t2 (to-dataset (for [{:keys [user event] :as m}
                             (second (second t1))]
                         (assoc m :user (u-byid user)
                                :event (e-byid event))))]
    t2))


(comment
(glab_submit
"/home/kiran/kaggle/eventcomp/data/pred4.csv"
"/home/kiran/kaggle/eventcomp/data/glab_submit4.csv"))





