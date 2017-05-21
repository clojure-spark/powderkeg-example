(ns powderkeg-example.core
  (:require
   [powderkeg.core :as keg]
   [net.cgrand.xforms :as x])
  (:import
   [org.apache.spark.sql SparkSession SQLContext]
   [org.apache.spark.sql.types StringType StructField StructType]
   [org.apache.spark.sql.types DataTypes]
   [org.apache.spark.sql Row SaveMode RowFactory]
   ))

(keg/connect! "local")

;; (def sqlContext (SQLContext. keg/*sc*))

;; (.address (Customer. 0 5 4 6 7)) ;;=> 7
(defrecord Customer [name gender ctfId  birthday address])

(def customer
  (let [txtf (.textFile keg/*sc* "/Users/clojure/Datas/2000W/100-test.csv")
        maped-rdd (keg/rdd
                   txtf
                   (map #(clojure.string/split % #",") )
                   (filter #(> (count %) 7) )

                   #_(map #(Customer. (nth % 0) (nth % 5) (nth % 4) (nth % 6)  (nth % 7)) )
                   #_(RowFactory/create (into-array (mapv ... )))
                   (map #(into-array (vector (nth % 0) (nth % 5) (nth % 4) (nth % 6)  (nth % 7))))
                   
;;                   ((fn [mrow]
;;                   ;;  (into-array
;;                      (map #(vector (nth % 0) (nth % 5) (nth % 4) (nth % 6)  (nth % 7)) mrow)
;;                   ;;  )
;;                      ))
                   ;;into-array
                   
                   ;;(RowFactory/create )
                   ;;(RowFactory/create (into-array [1 2 4 5 "66"]))
                   (distinct))]
    (into [] maped-rdd)
    #_(->
     (.createDataFrame
      (->> keg/*sc* .sc (new SparkSession))
      maped-rdd
      (DataTypes/createStructType
       (map #(DataTypes/createStructField % DataTypes/StringType false)
            ["name" "gender" "ctfId" "birthday" "address"]) )
      )
     )
    )
  
  )

(.createOrReplaceTempView customer "customer")
;;INFO SparkSqlParser: Parsing command: customer


(count
 (.sql
  (->> keg/*sc* .sc (new SQLContext))
  "SELECT * FROM customer"
  )

 ;;=> #object[org.apache.spark.sql.Dataset 0x3f5010cd "[name: string, gender: string ... 3 more fields]"]
 ;; INFO SparkSqlParser: Parsing command: SELECT * FROM customer
 
 )


;;(.collect customer)


;; (.printSchema customer) ;;===>>>
;; 在repl的那边显示
;; |-- name: string (nullable = false)
;; |-- gender: string (nullable = false)
;; |-- ctfId: string (nullable = false)
;; |-- birthday: string (nullable = false)
;; |-- address: string (nullable = false)
;;
