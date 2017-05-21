(ns powderkeg-example.core
  (:require
   [powderkeg.core :as keg]
   [net.cgrand.xforms :as x]))

(keg/connect! "local[2]")

(into [] ; no collect, plain Clojure
      (keg/rdd ["This is a firest line"  ; here we provide data from a clojure collection.
                "Testing spark"
                "and powderkeg"
                "Happy hacking!"]
               (filter #(.contains % "spark")))) ;;=> ["Testing spark"]

(into [] (keg/rdd (range 10))) ;;=> [0 1 2 3 4 5 6 7 8 9]

(keg/rdd (range 100)     ; source
         (filter odd?)          ; 1st transducer to apply
         (map inc)              ; 2nd transducer
         :partitions 2)         ; and options
;;  => #object[org.apache.spark.api.java.JavaRDD 0x15b0cac8 "MapPartitionsRDD[7] at mapPartitions at core.clj:339"]

(into [] (keg/scomp (take 5)) (range 10)) ;; => [0 1 2 3 4]
(keg/into [] (take 5) (range 10)) ;;=> [0 1 2 3 4]

(into [] (partition-by #(quot % 6)) (keg/rdd (range 20))) ;;=> [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17]]
(keg/into [] (partition-by #(quot % 6)) (keg/rdd (range 20))) ;;=> [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17] [18 19]]
(into [] (keg/scomp (partition-by #(quot % 6))) (keg/rdd (range 20))) ;;=> [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17] [18 19]]

(keg/rdd {:a 1 :b 2}) ;;=> #object[org.apache.spark.api.java.JavaRDD 0x528c1097 "MapPartitionsRDD[15] at mapPartitions at core.clj:339"]

(keg/rdd (range 100) (x/for [n %] [(mod n 7) n])) ;; => #object[org.apache.spark.api.java.JavaRDD 0x7ddce226 "MapPartitionsRDD[17] at mapPartitions at core.clj:339"]

(-> *1 .partitioner .orNull) ;;=> nil

(keg/by-key *1) ;;=> #object[org.apache.spark.api.java.JavaRDD 0x1b7c273f "ShuffledRDD[20] at partitionBy at core.clj:391"]

(-> *1 .partitioner .orNull) ;; => #object[org.apache.spark.HashPartitioner 0x353f93d4 "org.apache.spark.HashPartitioner@2"]

(keg/by-key (range 100) :key #(mod % 7)) ;;=> #object[org.apache.spark.api.java.JavaRDD 0x4231cbbf "ShuffledRDD[26] at partitionBy at core.clj:391"]

(-> *1 .partitioner .orNull) ;;=> #object[org.apache.spark.HashPartitioner 0x1317a756 "org.apache.spark.HashPartitioner@2"]

(into {}
      (keg/by-key (range 100) :key odd?
                  (x/reduce +))) ;;=> {false 2450, true 2500}

(into {}
      (keg/by-key {:a 1 :b 2}
                  (map inc))) ;;=> {:b 4, :a 3}

(into {}
      (keg/by-key (keg/by-key {:a 1 :b 2})
                  (map inc)
                  :shuffle nil)) ;;=> {:b 3, :a 2}
(into {}
      (keg/by-key {:a 1 :b 2}
                  (map inc)
                  :shuffle nil)) ;;=> {:a 2, :b 3}
(into {}
      (keg/by-key {:a 1 :b 2}
                  :pre (map inc))) ;;=> {:b 3, :a 2}

(into {}
      (keg/by-key {:a 1 :b 2}
                  :post (map inc))) ;;=> {:b 3, :a 2}

(into {}
      (keg/by-key (range 100) :key odd?
                  :pre (map inc)
                  (x/reduce +)
                  :post (map str))) ;;=> {false "2500", true "2550"}





