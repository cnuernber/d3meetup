(ns d3meetup.fit
  (:require [clojure.string :as s]
            [camel-snake-kebab.core :refer [->kebab-case]])
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
           [org.apache.commons.io FilenameUtils]))


(set! *warn-on-reflection* true)

(defrecord FileIdListener [listen-atom]
  FileIdMesgListener
  (onMesg [this mesg]
    (swap! listen-atom conj
           {:event-type :file-id
            :message-type (-> (.getType mesg)
                              (.getValue))
            :manufacturer (.getManufacturer mesg)
            :product (.getProduct mesg)
            :serial-number (.getSerialNumber mesg)
            :number (.getNumber mesg)})))

(defrecord UserProfileListener [listen-atom]
  UserProfileMesgListener
  (onMesg [this mesg]
    (swap! listen-atom conj
           {:event-type :file-id
            :friendly-name (.getFriendlyName mesg)
            :gender (case  (.getGender mesg)
                      Gender/MALE :male
                      Gender/FEMALE :female
                      :unknown)
            :age (.getAge mesg)
            :weight (.getWeight mesg)})))


(defrecord DevInfoListener [listen-atom]
  DeviceInfoMesgListener
  (onMesg [this mesg]
    (swap! listen-atom conj
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


(defrecord MonListener [listen-atom]
  MonitoringMesgListener
  (onMesg [this mesg]
    (swap! listen-atom conj
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
  [^Mesg msg field-num]
  (let [fields (.getOverrideField msg (short field-num))
        profile-field (Factory/createField (.getNum msg) (short field-num))
        prof-data (seq fields)]
    (when (and profile-field
               prof-data)
      {(keyword (->kebab-case (.getName profile-field)))
       (->> prof-data
            (mapv #(if (instance? Field %)
                     (.getValue ^Field %)
                     (.getValue ^FieldBase %))))})))


(def record-mesg-fields
  (->> (clojure.reflect/reflect RecordMesg)
       :members
       (map (fn [{:keys [flags name type declaring-class]}]

              (when (and (= flags #{:final :public :static})
                         (= declaring-class (symbol "com.garmin.fit.RecordMesg")))
                [(clojure.core/name name)
                 (-> (.getField RecordMesg (clojure.core/name name))
                     (.get nil))])))
       (remove nil?)
       (sort-by first)))


(defrecord RecListener [listen-atom]
  RecordMesgListener
  (onMesg [this mesg]
    (swap! listen-atom conj
           (apply merge
                  {:event-type :record-message}
                  (->> record-mesg-fields
                       (map (comp (partial record-data->seq mesg) second)))))))


(defn make-decode-listener
  [^Decode decoder listen-atom]
  (let [bcast
        (doto (MesgBroadcaster. decoder)
          (.addListener ^FileIdListener (->FileIdListener listen-atom))
          (.addListener ^UserProfileMesgListener (->UserProfileListener listen-atom))
          (.addListener ^DeviceInfoMesgListener (->DevInfoListener listen-atom))
          (.addListener ^MonitoringMesgListener (->MonListener listen-atom))
          (.addListener ^RecordMesgListener (->RecListener listen-atom)))]
    #(.read decoder ^InputStream % bcast bcast)))


(defn make-decoder
  ^Decode [listen-atom]
  (-> (Decode.)
      (make-decode-listener listen-atom)))


(def test-fname "data/activities/81623728.fit.gz")


(defn decode
  [fname]
  (let [gzip? (= "gz" (FilenameUtils/getExtension fname))
        listen-atom (atom [])
        decode-fn (make-decoder listen-atom)]
    (try
      (with-open [finstream (FileInputStream. (str fname))]
        (if gzip?
          (with-open [ginstream (GZIPInputStream. finstream)]
            (decode-fn ginstream))
          (decode-fn finstream)))
      (catch Throwable e (println e)))
    @listen-atom))
