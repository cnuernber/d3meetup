(ns d3meetup.doit
  (:require [d3meetup.fit :as fit]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.etl :as etl]
            [tech.compute.tensor.functional :as tens-fun]
            [clojure.core.matrix :as m]
            [clojure.set :as c-set]
            [oz.core :as oz])
  (:import [java.time Duration]))



(defn fit-file->dataset
  [fname]
  (ds/->dataset
   (->> (fit/decode fname)
        (filter #(= :record-message (:event-type %)))
        (map (fn [record-data]
               (->> (dissoc record-data :event-type)
                    (map (fn [[k v]]
                           [k (if (sequential? v)
                                (first v)
                                v)]))
                    (into {})))))
   {:table-name fname}))

(defn drop-missing
  [dataset]
  (let [missing-indexes (->> (ds/columns-with-missing-seq dataset)
                             (mapcat (fn [{:keys [column-name]}]
                                       (-> (ds/column dataset column-name)
                                           ds-col/missing)))
                             set)]
    (ds/select dataset :all  (->> (range (second (m/shape dataset)))
                                  (remove missing-indexes)))))


(defn ds-duration
  "Duration in seconds of the entire ride."
  [dataset]
  (let [{act-min :min
         act-max :max}
        (-> (ds/column dataset :timestamp)
            (ds-col/stats [:min :max]))]
    (Duration/ofSeconds
     (- (long act-max) (long act-min)))))

(def semi->deg
  (/ 180.0
     (Math/pow 2 31)))


(def load-pipeline
  '[[m= [:position-lat :position-long] (* (col) 8.381903171539307E-8)]])


(defn run-pipeline
  [dataset & {:keys [target] :as options}]
  (-> (drop-missing dataset)
      (etl/apply-pipeline load-pipeline options)))


(defonce test-ds (fit-file->dataset fit/test-fname))


(def processed-ds (run-pipeline test-ds))


(oz/view! [:div "one new thing"])
