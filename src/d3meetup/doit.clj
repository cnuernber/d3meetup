(ns d3meetup.doit
  (:require [cnuernber.garmin-fit :as fit]
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


(def lat-lon [:position-lat :position-long] )


(def load-pipeline
  [['m= lat-lon '(* (col) 8.381903171539307E-8)]
   '[m= :altitude-norm (/ (- (col :altitude) (min (col :altitude)))
                          (- (max (col :altitude)) (min (col :altitude))))]
   '[m= :speed-mph (* (col :speed) 2.23694)]])


(defn run-pipeline
  [dataset & {:keys [target] :as options}]
  (-> (drop-missing dataset)
      (etl/apply-pipeline load-pipeline options)
      :dataset
      ;;Make handling the data in the browser sane.
      (#(ds/ds-take-nth 10 %))
      (etl/apply-pipeline '[[m= :speed-avg (rolling 20 :mean (col :speed-mph))]
                            [m= :power-avg (rolling 20 :mean (col :power))]
                            [m= :cadence-avg (rolling 20 :mean (col :cadence))]
                            [m= :minutes-from-start (/ (- (col :timestamp)
                                                          (min (col :timestamp)))
                                                       60)]]
                          {})))


(defonce test-ds (fit-file->dataset fit/test-fname))


(def processed-pipeline (run-pipeline test-ds))

(def processed-ds (:dataset processed-pipeline))


(def all-the-data (-> (ds/select processed-ds
                                 (concat lat-lon
                                         [:timestamp :altitude :power :speed-mph
                                          :minutes-from-start
                                          :cadence :power-avg :speed-avg :cadence-avg])
                                 :all)
                      (ds/->flyweight)))

(def timestamp-data (ds-col/stats (ds/column processed-ds :timestamp)
                                  [:min :max]))

(def minutes-range (ds-col/stats (ds/column processed-ds :minutes-from-start)
                                  [:min :max]))

(def altitude-data (ds-col/stats (ds/column processed-ds
                                            :altitude)
                                 [:min :max]))

(def latitude-range (mapv (ds-col/stats (ds/column processed-ds :position-lat)
                                        [:min :max])
                          [:min :max]))

(def longitude-range (mapv (ds-col/stats (ds/column processed-ds :position-long)
                                         [:min :max])
                           [:min :max]))

(defn duration->str
  [^Duration dur]
  (.toString dur))

(def chart-width 600)
(def chart-height 150)

(def view-ds
  [:div
   [:h2 (format "Behold - %s - %s"
                (ds/dataset-name processed-ds)
                (duration->str (ds-duration processed-ds)))]
   [:h3 "Dashboard"]
   [:vega-lite {:data {:values all-the-data}
                :vconcat [{:projection {:type :albersUsa}
                           :width chart-width
                           :height chart-height
                           :mark :circle
                           :transform [{:filter {:selection :times}}]
                           :encoding {:latitude {:field (first lat-lon)
                                                 :type :quantitative}
                                      :longitude {:field (second lat-lon)
                                                  :type :quantitative}
                                      :color {:field :altitude
                                              :type :quantitative
                                              :scale {:range [:darkblue :lightblue]}}}}
                          {
                           :hconcat
                           [{:width (/ chart-width 2)
                             :height chart-height
                             :mark :point
                             :selection {:times {:type :interval}}
                             :encoding
                             {:x {:field :minutes-from-start
                                  :type :quantitative
                                  :scale {:domain
                                          [(:min minutes-range)
                                           (:max minutes-range)]}}
                              :y {:field :altitude
                                  :type :quantitative
                                  :scale {:domain [(:min altitude-data)
                                                   (:max altitude-data)]}}
                              :color {:field :altitude
                                      :type :quantitative
                                      :scale {:range [:darkblue :lightblue]}}}}
                            {:layer [{:width (/ chart-width 2)
                                      :height chart-height
                                      :mark :point
                                      :selection {:times {:type :interval}}
                                      :encoding {:x {:field :minutes-from-start
                                                     :type :quantitative
                                                     :scale {:domain
                                                             [(:min minutes-range)
                                                              (:max minutes-range)]}}
                                                 :y {:field :speed-mph
                                                     :type :quantitative}}}
                                     {:mark {:type :line
                                             :color :yellow}
                                      :encoding {:x {:field :minutes-from-start
                                                     :type :quantitative
                                                     :scale {:domain
                                                             [(:min minutes-range)
                                                              (:max minutes-range)]}}
                                                 :y {:field :speed-avg
                                                     :type :quantitative}}}]}]}
                          {:hconcat
                           [{:layer [{:width (/ chart-width 2)
                                      :height chart-height
                                      :mark :point
                                    :selection {:times {:type :interval}}
                                    :encoding {:x {:field :minutes-from-start
                                                   :type :quantitative
                                                   :scale {:domain
                                                           [(:min minutes-range)
                                                            (:max minutes-range)]}}
                                               :y {:field :power
                                                   :type :quantitative}}}
                                     {:width (/ chart-width 2)
                                      :height chart-height
                                      :mark {:type :line
                                           :color :yellow}
                                    :encoding {:x {:field :minutes-from-start
                                                   :type :quantitative
                                                   :scale {:domain
                                                           [(:min minutes-range)
                                                            (:max minutes-range)]}}
                                               :y {:field :power-avg
                                                   :type :quantitative}}}]}
                            {:layer [{:width (/ chart-width 2)
                                      :height chart-height
                                      :mark :point
                                      :selection {:times {:type :interval}}
                                      :encoding {:x {:field :minutes-from-start
                                                     :type :quantitative
                                                     :scale {:domain
                                                             [(:min minutes-range)
                                                              (:max minutes-range)]}}
                                                 :y {:field :cadence
                                                     :type :quantitative}}}
                                     {:width (/ chart-width 2)
                                      :height chart-height
                                      :mark {:type :line
                                             :color :yellow}
                                      :encoding {:x {:field :minutes-from-start
                                                     :type :quantitative
                                                     :scale {:domain
                                                             [(:min minutes-range)
                                                              (:max minutes-range)]}}
                                                 :y {:field :cadence-avg
                                                     :type :quantitative}}}]}]}

                          ]}]])


(oz/view! view-ds)
