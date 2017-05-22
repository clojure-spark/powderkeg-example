(ns powderkeg-example.sql
  (:require
   [powderkeg.core :as keg]
   [net.cgrand.xforms :as x])
  (:import
   [org.apache.spark.sql SparkSession SQLContext]
   [org.apache.spark.sql.types StringType StructField StructType]
   [org.apache.spark.sql.types DataTypes]
   [org.apache.spark.sql Row SaveMode RowFactory] ))

(keg/connect! "local")

(def customer
  (let [txtf (.textFile keg/*sc* "/Users/clojure/Datas/2000W/100-test.csv")
        maped-rdd (keg/rdd
                   txtf
                   (map #(clojure.string/split % #",") )
                   (filter #(> (count %) 7) )
                   (map #(RowFactory/create
                          (into-array
                           (mapv % [0 5 4 6 7])
                           )))
                   (distinct))]
    (.createDataFrame
     (->> keg/*sc* .sc (new SparkSession))
     maped-rdd
     (DataTypes/createStructType
      (map #(DataTypes/createStructField % DataTypes/StringType false)
           ["name" "gender" "ctfId" "birthday" "address"]) )
     )
    )
  )

(comment
  (.createOrReplaceTempView customer "customer")
  (.show
   (.sql
    (->> keg/*sc* .sc (new SQLContext))
    "SELECT * FROM customer LIMIT 10"
    )
   )
  )

