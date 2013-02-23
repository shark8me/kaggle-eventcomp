(ns ecomp2.haversine)

 ;from https://gist.github.com/1787462

; http://en.wikipedia.org/wiki/Earth_radius 
(def R 6367)
 
; rad argument to haversine function (180° d/πR).
(defn rad [x] 
  (* x  (/ Math/PI 180)))
 
; haversine function 
(defn 
  #^{:test (fn []
       (assert (= 1.2920680792922851 (haversine
    {:lat 55.449780 :lon 11.823392} {:lat 55.451021 :lon 11.803007} ))))}
  haversine 
  [position destination]
  "Calculate the distance between two coordinates, with the haversine formula"
  (let [square_half_chord 
          (+ (Math/pow (Math/sin (/ (rad (- (destination :lat)(position :lat))) 2)) 2) 
             (* (Math/cos (rad (position :lat))) 
                (Math/cos (rad (destination :lat))) 
                (Math/pow (Math/sin (/ (rad (- (destination :lon)(position :lon))) 2)) 2)))
        angular_distance 
          (* (Math/asin (Math/sqrt square_half_chord)) 2) ]
    (* angular_distance R)))
 
(defn meter [x] (* x 1000))
 
(def p1 {:name "Artilleristen" :lat 55.449780 :lon 11.823392})
(def p2 {:name "Rundkørslen"   :lat 55.451021 :lon 11.803007}) 

;(meter (haversine p1 p2))