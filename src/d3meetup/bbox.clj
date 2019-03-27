(ns d3meetup.bbox
  (:require [cnuernber.garmin-fit :as fit]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [clojure.core.matrix :as m]
            [tech.ml.dataset.etl :as etl]
            [clojure.java.io :as io]
            [tech.parallel :as parallel]
            [tech.io :as tech-io])
  (:import [java.io File]))

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


(def semi->deg
  (/ 180.0
     (Math/pow 2 31)))


(def lat-lon [:position-lat :position-long] )


(def load-pipeline
  [['m= lat-lon (list '* (list 'col) semi->deg)]])


(defn drop-missing
  [dataset]
  (let [missing-indexes (->> (ds/columns-with-missing-seq dataset)
                             (mapcat (fn [{:keys [column-name]}]
                                       (-> (ds/column dataset column-name)
                                           ds-col/missing)))
                             set)]
    (ds/select dataset :all  (->> (range (second (m/shape dataset)))
                                  (remove missing-indexes)))))


(defn run-pipeline
  [dataset & {:keys [target] :as options}]
  (-> (drop-missing dataset)
      (etl/apply-pipeline load-pipeline options)
      :dataset))


(def fname->bbox
  (parallel/memoize
   (fn [fname]
     (let [dataset (-> (fit-file->dataset fname)
                       (run-pipeline))]
       {:latitude (-> (ds/column dataset :position-lat)
                      (ds-col/stats [:min :max]))
        :longitude (-> (ds/column dataset :position-long)
                       (ds-col/stats [:min :max]))}))))


(def all-fit-files
  (->> (file-seq (io/file "data/activities"))
       rest
       (filter #(.endsWith (.getPath %) "fit.gz"))))


(defn process-fit-files
  []
  (->> all-fit-files
       (map-indexed vector)
       (pmap (fn [[idx ^File fdata]]
               (when (= 0 (rem idx 100))
                 (println "Processing file" idx))
               (try
                 [(.getPath fdata)
                  (fname->bbox (.getPath fdata))]
                 (catch Throwable e (println e) nil))))
       (remove nil?)
       (into {})))

(def fit-filemap
  (memoize
   (fn []
     (process-fit-files))))

(def file-url "file://data/activities/bboxes.nippy")

(def load-filemap
  (memoize
   (fn []
     (when-not (.exists (tech-io/file file-url))
       (let [fmap (fit-filemap)]
         (tech-io/put-nippy! file-url fmap)))
     (tech-io/get-nippy file-url))))

(defn ll-bbox->xy-bbox
  [{:keys [latitude longitude]}]
  [(:min longitude) (:min latitude)
   (:max longitude) (:max latitude)])

(defn area
  [bbox]
  (let [[l-x-min l-y-min l-x-max l-y-max] (ll-bbox->xy-bbox bbox)]
    (* (- l-x-max l-x-min)
       (- l-y-max l-y-min))))

(defn intersection
  [lhs-bbox rhs-bbox]
  (let [[l-x-min l-y-min l-x-max l-y-max] (ll-bbox->xy-bbox lhs-bbox)
        [r-x-min r-y-min r-x-max r-y-max] (ll-bbox->xy-bbox rhs-bbox)
        x-overlap (max 0 (- (min l-x-max r-x-max) (max l-x-min r-x-min)))
        y-overlap (max 0 (- (min l-y-max r-y-max) (max l-y-min r-y-min)))]
    (* x-overlap y-overlap)))


(defn union
  [lhs-bbox rhs-bbox]
  (- (+ (area lhs-bbox) (area rhs-bbox))
     (intersection lhs-bbox rhs-bbox)))


(defn intersection-over-union
  [lhs-bbox rhs-bbox]
  (/ (intersection lhs-bbox rhs-bbox)
     (union lhs-bbox rhs-bbox)))


(defn nearest-boxes
  [fname]
  (let [fmap (load-filemap)
        target (get fmap fname)]
    (when-not target
      (throw (ex-info (format "Failed to find target %s" target)
                      {:target target})))
    (->> fmap
         (pmap (fn [[fname bbox]]
                [fname (intersection-over-union target bbox)]))
         (sort-by second >))))


(defn dataset->latlong
  [dataset]
  (-> (ds/select dataset (concat [:position-lat :position-long]) :all)
      (ds/->flyweight)))


(defn compare-paths
  [fnames]
  (let [values (->> fnames
                    (pmap (fn [left-fname]
                            (->> (-> (fit-file->dataset left-fname)
                                     (run-pipeline)
                                     dataset->latlong)
                                 (map #(assoc % :file left-fname))
                                 (take-nth 4))))
                    (apply concat))]
    [:vega-lite {:data {:values values}
                 :width 800
                 :height 600
                 :projection {:type :albersUsa}
                 :mark {:type :circle}
                 :encoding {:latitude {:field (first lat-lon)
                                       :type :quantitative}
                            :longitude {:field (second lat-lon)
                                        :type :quantitative}
                            :color {:field :file
                                    :type :nominal}}}]))


(defn compare-file-paths
  [fname]
  (->> (nearest-boxes fname)
       (take 4)
       (map first)
       compare-paths))


(defn compare-random
  []
  (->> (keys (load-filemap))
       shuffle
       first
       compare-file-paths))
