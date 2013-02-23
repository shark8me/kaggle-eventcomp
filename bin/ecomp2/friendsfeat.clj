(ns ecomp2.friendsfeat)
(use '(incanter core io mongodb))
(use 'somnium.congomongo)
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])
(use '[ecomp2.mfeat :as mfeat])
(use '[ecomp2.glab :as glab])
;creates a matrix of user's friends and then gets a reduced dimention
;representation of each user('s friends)

(defn get-all-users []
  "returns a set containing users in train,test,user.csv and user_friends.csv"
  (let [ufn (fn [userkey x] (pmap userkey (fetch x :only [userkey])))
        utraintest (reduce into #{} (pmap  (partial ufn :user) [:train_csv :test_csv])) 
        from_users_csv (reduce into #{} (pmap (partial ufn :user_id) [:users_csv]))
        srcusers (reduce into #{} (pmap (partial ufn :user) [:user_friends_csv]))
        ;dstusers (reduce #(into %1 
        ;              (if (sequential? %2) %2 [%2])) #{}
        ;                 (pmap :friends 
        ;(fetch :user_friends_csv :only [:friends])))
        ]
     (reduce into utraintest [from_users_csv srcusers ])))

;(take 5 (get-all-users))

(defn write-useq-to-db []
  "create a collection that maps the user id against a running sequence of ids"
  (let [useq (get-all-users)]
    (drop-coll! :user_seq)
    (count (pmap #(insert! :user_seq {:u %1 :uid %2}) useq (iterate inc 0)))
    (add-index! :user_seq [:u])
    (add-index! :user_seq [:uid])))

(defn create-user-friend-sparse-coll []
  "inserts into the :user-friends coll the ids of users who are friends"
  (do (drop-coll! :user-friends)
        (count 
          (for [{:keys [user friends] :as m}   (fetch :user_friends_csv)]
            (let [ uidsrc (glab/uid user)
                  uidf (filter #(not (nil? %))
                               (if (sequential? friends) 
                                 (for [f friends] (glab/uid f)) [friends]))]        
              (mass-insert! :user-friends (pmap #(assoc {} :u uidsrc :f %) uidf)))))))

(defn create-user-friend-sparse-coll3 []
  "inserts into the :user-friends coll the ids of users who are friends
   parallel version"
  (do (drop-coll! :user-friends2)
    (count (let [fnx  (fn [{:keys [user friends] :as m}] 
                 (let [ uidsrc (glab/uid user)
                       uidf (filter #(not (nil? %))
                                    (pmap glab/uid
                                          (if (sequential? friends) 
                                            friends [friends])))]        
                   (mass-insert! :user-friends2 (pmap #(assoc {} :u uidsrc :f %) uidf))))]
      (pmap fnx (fetch  :user_friends_csv))))))

(comment (mongo! :db "seconddb")
         (add-index! :event_seq  [:e])
         (add-index! :event_seq  [:eid])
    (add-index! :user_seq [:u])
    (add-index! :user_seq [:uid]))

(comment 
  (write-useq-to-db)
  (create-user-friend-sparse-coll3)
  (save (to-dataset (map #(dissoc % :_id)
                       (fetch :user-friends2)))
      "/home/kiran/kaggle/eventcomp/data/userfriend.csv")
  )

(comment 
  (def p1 (read-dataset
          "/home/kiran/kaggle/eventcomp/data/pcaout.csv"
          :header true))
  (count (map 
  #(insert! :pca (assoc %1 :u %2 :user (ecomp2.glab/u-byid %2)))
  (second (second p1)) (iterate inc 1))))