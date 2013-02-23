(ns ecomp2.loadfiles)


(defn plong [s] (Long/parseLong s))
(defn pdoub [s] (if (= 0 (count s)) nil (Double/parseDouble s)))

(defn get-long-set [iseq]
  "runs conversion to long on all elements in iseq "
  (set (for [i iseq] (plong i))))

(defn load-user_friends_csv [filename]
  "returns a map, key is user-id, value is set of user-id's friends 
loaded from user_friends.csv"
  (let  [fin  (userparse/readfile filename )
         fnx (fn [mapm linein ] 
               (let [arr (.split linein "," 2)] 
                 ;(println (str "lufc " (first arr)))
                 (assoc mapm (plong (first arr)) 
                        (let [sin (second arr)]
                          (if (= 0 (.length sin)) #{} (get-long-set (.split (second arr) " ")))))))]
    (reduce fnx {} (rest fin))))

(defn load-event_attendees_csv [filename]
  "returns a map, key is event-id, value is a list of sets, yes,maybe,invited,no,
in that order"
  (let  [fin  (userparse/readfile filename )
         ymin (fn [s] (if (= 0 (.length s)) #{} (get-long-set (.split s " "))))
         fnx (fn [mapm linein ] 
               (let [arr (.split linein "," 5)]                 
                 (assoc mapm (plong (first arr)) 
                        (let [inx (rest arr)] 
                          {:yes (ymin (first inx)) :maybe (ymin (second inx))
                           :invited (ymin (nth inx 2)) :no (ymin (last inx))}))))]
    (reduce fnx {} (rest fin))))

(defn load-train_csv [filename]
  "returns a list, each entry is a tuple containing userid, eventid"
  (let [fin (userparse/readfile filename)
        fnx (fn [s] (let [arr (.split s ",")]
              (zipmap [:user-id :event-id] (map plong (take 2 arr))))) ]
    (map fnx (rest fin))))
