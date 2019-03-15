(ns d3meetup.fit
  (:require [clojure.string :as s]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [clojure.core.async :as async]
            [tech.parallel :as parallel]
            [clojure.java.io :as io])
  (:import [com.garmin.fit Fit Decode
            FileIdMesg FileIdMesgListener
            UserProfileMesg UserProfileMesgListener
            DeviceInfoMesg DeviceInfoMesgListener
            MonitoringInfoMesg MonitoringInfoMesgListener
            MonitoringMesg MonitoringMesgListener
            RecordMesg RecordMesgListener
            DeveloperFieldDescription DeveloperFieldDescriptionListener
            Gender BatteryStatus Mesg
            Factory Field FieldBase
            MesgBroadcaster]
           [java.io FileInputStream File InputStream]
           [java.util.zip GZIPInputStream]
           [java.util Date]
           [org.apache.commons.io FilenameUtils]))


(set! *warn-on-reflection* true)

(defrecord FileIdListener [result-chan]
  FileIdMesgListener
  (onMesg [this mesg]
    (async/>!! result-chan
              {:event-type :file-id
               :message-type (-> (.getType mesg)
                                 (.getValue))
               :manufacturer (.getManufacturer mesg)
               :product (.getProduct mesg)
               :serial-number (.getSerialNumber mesg)
               :number (.getNumber mesg)})))

(defrecord UserProfileListener [result-chan]
  UserProfileMesgListener
  (onMesg [this mesg]
    (async/>!! result-chan
           {:event-type :file-id
            :friendly-name (.getFriendlyName mesg)
            :gender (case  (.getGender mesg)
                      Gender/MALE :male
                      Gender/FEMALE :female
                      :unknown)
            :age (.getAge mesg)
            :weight (.getWeight mesg)})))


(defrecord DevInfoListener [result-chan]
  DeviceInfoMesgListener
  (onMesg [this mesg]
    (async/>!! result-chan
           (merge
            {:event-type :device-info
             :batter-status (case (.getBatteryStatus mesg)
                              BatteryStatus/CRITICAL :critical
                              BatteryStatus/GOOD :good
                              BatteryStatus/LOW :low
                              BatteryStatus/NEW :new
                              BatteryStatus/OK :ok
                              :invalid)}
            (when-let [timestamp (.getTimestamp mesg)]
              {:timestamp timestamp})))))


(defrecord MonListener [result-chan]
  MonitoringMesgListener
  (onMesg [this mesg]
    (async/>!! result-chan
           (merge
            {:event-type :monitoring-message}
            (when-let [ts (.getTimestamp mesg)]
              {:timestamp ts})
            (when-let [act-type (.getActivityType mesg)]
              {:activity-type (.getActivityType mesg)})
            (when-let [steps (.getSteps mesg)]
              {:steps steps})
            (when-let [strokes (.getStrokes mesg)]
              {:strokes strokes})
            (when-let [cycles (.getCycles mesg)]
              {:cycles cycles})))))


(defn- record-data->seq
  [^Mesg msg {:keys [field-name field-units field-index field-scale field-offset]}]
  (let [fields (.getOverrideField msg (short field-index))
        profile-field (Factory/createField (.getNum msg) (short field-index))
        prof-data (seq fields)]
    (when (and profile-field
               prof-data)
      [(keyword (->kebab-case (.getName profile-field)))
       (->> prof-data
            (mapv #(if (instance? Field %)
                     (.getValue ^Field %)
                     (.getValue ^FieldBase %))))])))


(def record-mesg-fields
  (let [rec-msg-field (doto (.getDeclaredField RecordMesg "recordMesg")
                        (.setAccessible true))
        ^Mesg record-msg-obj (.get rec-msg-field nil)
        ^java.lang.reflect.Field off-field (doto (.getDeclaredField Field "offset")
                                             (.setAccessible true))
        ^java.lang.reflect.Field scale-field (doto (.getDeclaredField Field "offset")
                                               (.setAccessible true))
        ^java.lang.reflect.Field name-field (doto (.getDeclaredField Field "name")
                                              (.setAccessible true))
        ]
    (->> (clojure.reflect/reflect RecordMesg)
         :members
         (map (fn [{field-name-symbol :name
                    field-type :type
                    declaring-class :declaring-class
                    flags :flags}]
                (when (and (= flags #{:final :public :static})
                           (= declaring-class (symbol "com.garmin.fit.RecordMesg")))
                  (let [
                        field-index (-> (.getField RecordMesg (name field-name-symbol))
                                        (.get nil))
                        ^com.garmin.fit.Field field (.getField record-msg-obj (int field-index))
                        field-name (keyword (->kebab-case (.get name-field field)))
                        ]
                    [field-name
                     (merge {:field-index (-> (.getField RecordMesg (name field-name-symbol))
                                              (.get nil))
                             :field-name field-name
                             :is-accumulated? (.getIsAccumulated field)}
                            (when (not= "" (.getUnits field))
                              {:field-units (keyword (->kebab-case (.getUnits field)))})
                            (when (not= 0.0 (.get off-field field))
                              {:field-offset (.get off-field field)})
                            (when (not= 0.0 (.get scale-field field))
                              {:field-scale (.get scale-field field)}))]))))
         (remove nil?)
         (into {}))))


;; While the following may not be useful to most people, in the event that you need to
;; convert FIT file timestamps produced by a Garmin product, this is a time-saver. For
;; whatever reason, Garmin created their own epoch, which began at midnight on Sunday, Dec
;; 31st, 1989 (the year of Garmin’s founding). And as far as I know, all Garmin products
;; log data using this date as a reference. The conversion is simple, just add 631065600
;; seconds to a Garmin numeric timestamp, then you have the number of seconds since the
;; 1970 Unix epoch, which is far more common.
(def fit-timestamp-seconds-base 631065600)

(defn timestamp->date
  ^Date [^long ts]
  (Date. (* 1000 (+ ts (long fit-timestamp-seconds-base)))))

(defrecord RecListener [result-chan]
  RecordMesgListener
  (onMesg [this mesg]
    (async/>!! result-chan
               (->> (concat
                     [[:event-type :record-message]]
                     (->> record-mesg-fields
                          (map (comp (partial record-data->seq mesg) second))
                          (remove nil?)))
                    (into {})))))


(defn make-decode-listener
  [^Decode decoder result-chan]
  (let [bcast
        (doto (MesgBroadcaster. decoder)
          (.addListener ^FileIdListener (->FileIdListener result-chan))
          (.addListener ^UserProfileMesgListener (->UserProfileListener result-chan))
          (.addListener ^DeviceInfoMesgListener (->DevInfoListener result-chan))
          (.addListener ^MonitoringMesgListener (->MonListener result-chan))
          (.addListener ^RecordMesgListener (->RecListener result-chan)))]
    #(.read decoder ^InputStream % bcast bcast)))


(defn make-decoder
  ^Decode [result-chan]
  (-> (Decode.)
      (make-decode-listener result-chan)))


(def test-fname "data/activities/81623728.fit.gz")


(defn decode
  "Returns a lazy sequence of records from the file."
  [fname]
  (let [gzip? (= "gz" (FilenameUtils/getExtension fname))
        record-chan (async/chan 64)
        decode-fn (make-decoder record-chan)]
    (when-not (.exists (io/file fname))
      (throw (ex-info "File does not exist." {})))
    (async/thread
      (try
        (with-open [finstream (FileInputStream. (str fname))]
          (if gzip?
            (with-open [ginstream (GZIPInputStream. finstream)]
              (decode-fn ginstream))
            (decode-fn finstream)))
        (catch Throwable e (println e) nil))
      (async/close! record-chan))
    (parallel/async-channel-to-lazy-seq record-chan)))
