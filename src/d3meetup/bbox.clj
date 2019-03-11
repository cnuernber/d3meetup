(ns d3meetup.bbox
  (:require [d3meetup.fit :as fit]
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
      (etl/apply-pipeline load-pipeline options)))


(def fname->bbox
  (parallel/memoize
   (fn [fname]
     (let [dataset (-> (fit-file->dataset fname)
                       (run-pipeline)
                       :dataset)]
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
