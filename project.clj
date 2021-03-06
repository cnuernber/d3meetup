(defproject d3meetup "0.1.0-SNAPSHOT"
  :description "Exploration of the brian lehman dataset"
  :url "http://github.com/cnuernber/d3meetup"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [techascent/tech.ml "0.27"]
                 [org.clojure/tools.reader "1.3.2"]
                 [metasoarous/oz "1.6.0-alpha2"]
                 [cnuernber/garmin-fit-clj "0.1"]]

  :resource-paths ["resources/fit.jar"])
