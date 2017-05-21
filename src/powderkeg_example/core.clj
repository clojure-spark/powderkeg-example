(ns powderkeg-example.core
  (:require
   [powderkeg.core :as keg]
   [net.cgrand.xforms :as x])
  (:import
   [org.apache.spark.sql SparkSession]
   [org.apache.spark.sql.types StringType StructField StructType]
   [org.apache.spark.sql.types DataTypes]
   [org.apache.spark.sql Row SaveMode RowFactory]
   ))

(keg/connect! "local")

(def sqlContext (SQLContext. keg/*sc*))
(def spark-session (->> keg/*sc* .sc (new SparkSession)))

;; (.address (Customer. 0 5 4 6 7)) ;;=> 7
(defrecord Customer [name gender ctfId  birthday address])

(def customer
  (let [txtf (.textFile keg/*sc* "/Users/clojure/Datas/2000W/100-test.csv")
        maped-rdd (keg/rdd
                   txtf
                   (map #(clojure.string/split % #",") )
                   (filter #(> (count %) 7) )
                   (map #(Customer. (nth % 0) (nth % 5) (nth % 4) (nth % 6)  (nth % 7)) )
                   (distinct))]
    (->
     (.createDataFrame
      spark-session maped-rdd
      (DataTypes/createStructType
       (map #(DataTypes/createStructField % DataTypes/StringType false)
            ["name" "gender" "ctfId" "birthday" "address"]) )
      )
     )
    )
  )
