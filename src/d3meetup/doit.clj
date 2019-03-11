(ns d3meetup.doit
  (:require [d3meetup.fit :as fit]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.etl :as etl]
            [tech.compute.tensor.functional :as tens-fun]
            [clojure.core.matrix :as m]
            [clojure.set :as c-set]
            [oz.core :as oz]
            ;; [clojure.pprint :as pp]
            )
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


(def lat-lon [:position-lat :position-long] )


(def load-pipeline
  [['m= lat-lon '(* (col) 8.381903171539307E-8)]])


(defn run-pipeline
  [dataset & {:keys [target] :as options}]
  (-> (drop-missing dataset)
      (etl/apply-pipeline load-pipeline options)))


(defonce test-ds (fit-file->dataset fit/test-fname))


(def processed-pipeline (run-pipeline test-ds))

(def processed-ds (:dataset processed-pipeline))

(def lat-lon-data (-> (ds/select processed-ds (concat lat-lon
                                                                 [:timestamp]) :all)
                      (ds/->flyweight)))

(def effort-speed-height-time (-> (ds/select processed-ds
                                             [:timestamp :altitude :power :speed
                                              :cadence]
                                             :all)
                                  (ds/->flyweight)))

(def timestamp-data (ds-col/stats (ds/column processed-ds :timestamp)
                                  [:min :max]))

(def altitude-data (ds-col/stats (ds/column processed-ds
                                            :altitude)
                                 [:min :max]))

(defn duration->str
  [^Duration dur]
  (.toString dur))

(def view-ds [:div
              [:h2 (format "Behold - %s - %s"
                           (ds/dataset-name processed-ds)
                           (duration->str (ds-duration processed-ds)))]
              [:h3 "Geo Data"]
              [:vega-lite {:data {:values lat-lon-data}
                           :width 800
                           :height 600
                           :projection {:type :albersUsa}
                           :mark :circle
                           :encoding {:latitude {:field (first lat-lon)
                                                 :type :quantitative}
                                      :longitude {:field (second lat-lon)
                                                  :type :quantitative}}}]
              [:h3 "Graphs"]
              [:div
               [:div
                [:vega-lite {:repeat {:row [:altitude]}
                             :spec {:data {:values effort-speed-height-time}
                                    :width 800
                                    :mark :point
                                    :encoding
                                    {:x {:field :timestamp
                                         :type :quantitative
                                         :scale {:domain
                                                 [(:min timestamp-data)
                                                  (:max timestamp-data)]}}
                                     :y {:field :altitude
                                         :type :quantitative
                                         :scale {:domain [(:min altitude-data)
                                                          (:max altitude-data)]}}}}}]]
               [:div
                [:vega-lite {:repeat {:row [:power :speed :cadence]}
                             :spec {:data {:values effort-speed-height-time}
                                    :width 800
                                    :mark :point
                                    :encoding {:x {:field :timestamp
                                                   :type :quantitative
                                                   :scale {:domain
                                                           [(:min timestamp-data)
                                                            (:max timestamp-data)]}}
                                               :y {:field {:repeat :row}
                                                   :type :quantitative}}}}]]]])


(oz/view! view-ds)
