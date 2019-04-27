(ns clojurewerkz.langohr.examples.recovery.example1
  (:gen-class)
  (:require [langohr.core      :as rmq]
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

(defn start-consumer
  [ch ^String q]
  (lq/declare ch q {:exclusive false :auto-delete false})
    (lc/subscribe ch q message-handler {:auto-ack true}))

(defn -main
  [& args]
  (let [conn (rmq/connect {:automatically-recover true :automatically-recover-topology true})
        q    "langohr.examples.recovery.example1.q"
        x    ""]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    (start-consumer (lch/open conn) q)
    (rmq/on-recovery (lch/open conn) (fn [ch]
                          (println "[main] Channel recovered. Recovering topology...")
                          (start-consumer (lch/open conn) q)))
    (while true
      (Thread/sleep 1000)
      (try
        (println "publishing a message: hello")
        (lb/publish (lch/open conn) x q "hello")
        (catch AlreadyClosedException ace
          (comment "Happens when you publish while the connection is down"))
        (catch IOException ioe
          (comment "ditto"))))))

