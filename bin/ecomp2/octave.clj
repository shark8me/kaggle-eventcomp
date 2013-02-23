(ns ecomp2.octave)
(use '(incanter core io mongodb))
(use 'somnium.congomongo)
(use '[ecomp2.loadds :as lf])
(use '[ecomp2.files :as d])
(use '[ecomp2.mfeat :as mfeat])

;creates input to run logistic regression in octave

