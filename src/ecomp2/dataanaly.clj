(ns ecomp2.dataanaly)

(use '(incanter core io mongodb charts))
(use 'somnium.congomongo)

(def ssds (read-dataset 
  "/home/kiran/Downloads/ss06pid.csv"
  :header true))

(col-names ssds)

(view (box-plot :AGEP :data ssds ))