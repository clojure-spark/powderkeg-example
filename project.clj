(defproject powderkeg-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [hcadatalab/powderkeg "0.5.1"]
                 [com.esotericsoftware/kryo-shaded "4.0.0"]  ;; For Spark 2.x support
                 [org.apache.spark/spark-core_2.11 "2.1.0"]
                 [org.apache.spark/spark-sql_2.11 "2.1.0"]
                 [org.apache.spark/spark-streaming_2.11 "2.1.0"]
                 [org.apache.spark/spark-mllib_2.11 "2.1.0"]
                 [clj-time "0.12.2"]]
  :aot :all
  :repositories {"aliyun" "http://maven.aliyun.com/nexus/content/groups/public"})
