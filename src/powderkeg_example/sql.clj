(ns powderkeg-example.sql
  (:require
   [powderkeg.core :as keg]
   [net.cgrand.xforms :as x])
  (:import
   [org.apache.spark.sql SparkSession SQLContext]
   [org.apache.spark.sql.types StringType StructField StructType]
   [org.apache.spark.sql.types DataTypes]
   [org.apache.spark.sql Row SaveMode RowFactory] ))

;; "local[*]" 连接本地的spark
(keg/connect! "local")

(def customer
  (let [;; 读取某个目录的某些文件*.csv: 返回JavaRDD
        txtf (.textFile keg/*sc* "/Users/clojure/Datas/2000W/100-test.csv")
        ;; Powderkeg: 实时，不用aot，原生transducer处理数据, (keg/rdd (map ...) (filter ...) (mapcat ...)). keg/rdd是transducer, 就像"->"一样使用
        ;; 测试或者转为Clojure输出: 要(into [] (keg/rdd ...))转为Clojure处理
        maped-rdd (keg/rdd
                   txtf
                   (map #(clojure.string/split % #",") )
                   (filter #(> (count %) 7) )
                   (map
                    ;; sql就是每一行都是row对象, Scala里面一般会建立一个类设置多个属性来对应它, Clojure 版本里面要转成Row才行(用defrecord无效)
                    #(RowFactory/create
                      ;; 转换为Java的数组
                      (into-array
                       ;; 取出第 0 5 4 6 7 列的数据
                       (mapv % [0 5 4 6 7])
                       )))
                   (distinct))]
    ;; 要把 Java的RDD变成 dataframe , 就要用 createDataFrame
    ;; 要传rdd及Schema(StructField)
    (.createDataFrame
     ;; keg/*sc* 就是Java的Spark源码的 sc
     ;; 从javaSparkSession得到scala版本的SparkSession
     (->> keg/*sc* .sc (new SparkSession))
     maped-rdd
     ;; 用StructField来创建StructType
     (DataTypes/createStructType
      (map #(DataTypes/createStructField % DataTypes/StringType false)
           ;;多个平行的字段
           ["name" "gender" "ctfId" "birthday" "address"]) )
     )
    )
  )

(comment
  ;; 注册table表格
  (.createOrReplaceTempView customer "customer")
  ;; 最终show出来,打印在repl里面,不会再Emacs返回
  (.show
   ;; 用dataframe和sql是差不多的, sql基于dataframe
   (.sql
    ;; SparkContext是基于rdd, SparkSession是基于DataFrame的
    ;; 从javaSparkContext得到scala版本的SparkContext
    (->> keg/*sc* .sc (new SQLContext))
    "SELECT * FROM customer LIMIT 10"
    )
   )
  )

