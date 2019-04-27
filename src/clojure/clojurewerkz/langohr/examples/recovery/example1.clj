(ns clojurewerkz.langohr.examples.recovery.example1
  (:gen-class)
  (:require [langohr.core      :as rmq]
            [mount.core :as mount :refer [defstate start]]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as lx]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb])
  (:import java.io.IOException
           com.rabbitmq.client.AlreadyClosedException))

(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (println (format "[consumer] Received a message: %s"
                   (String. payload "UTF-8"))))


(defn- start-connection []
  (rmq/connect {:automatically-recover true :automatically-recover-topology true}))

(defn- stop-connection [conn]
  (rmq/close conn))

(defstate connection
          :start (start-connection)
          :stop (stop-connection connection))

(defn declare-queue
  [^String q]
  (lq/declare (lch/open connection) q {:exclusive false :auto-delete false}))

(defn start-consumer-on-queue
  [^String q]
  (lc/subscribe (lch/open connection) q message-handler {:auto-ack true}))

(defn -main
  [& args]
  (mount/start #'connection)
  (let [q    "langohr.examples.recovery.example1.q"
        x    ""]
    (declare-queue q)
    (start-consumer-on-queue q)

    (while true
      (Thread/sleep 1000)
      (try
        (println "publishing a message: hello")
        (lb/publish (lch/open connection) x q "hello")
        (catch AlreadyClosedException ace
          (comment "Happens when you publish while the connection is down"))
        (catch IOException ioe
          (comment "ditto"))))))