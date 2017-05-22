//Spark 2.1.1的版本: http://colobu.com/2014/12/11/spark-sql-quick-start/
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

case class Customer(name: String, gender: String, ctfId: String, birthday: String, address: String)
val customer = sc.textFile("/Users/clojure/Datas/2000W/*.csv").map(_.split(",")).filter(line => line.length > 7).map(p => Customer(p(0), p(5), p(4), p(6), p(7))).distinct().toDF()
customer.registerTempTable("customer")

def toInt(s: String):Int = {
  try {
    s.toInt
  } catch {
    case e:Exception => 9999
  }
}

def myfun(birthday: String) : String = {
  var rt = "未知"
  if (birthday.length == 8) {
    val md = toInt(birthday.substring(4))
    if (md >= 120 & md <= 219)
      rt = "水瓶座"
    else if (md >= 220 & md <= 320)
      rt = "双鱼座"
    else if (md >= 321 & md <= 420)
      rt = "白羊座"
    else if (md >= 421 & md <= 521)
      rt = "金牛座"
    else if (md >= 522 & md <= 621)
      rt = "双子座"
    else if (md >= 622 & md <= 722)
      rt = "巨蟹座"
    else if (md >= 723 & md <= 823)
      rt = "狮子座"
    else if (md >= 824 & md <= 923)
      rt = "处女座"
    else if (md >= 924 & md <= 1023)
      rt = "天秤座"
    else if (md >= 1024 & md <= 1122)
      rt = "天蝎座"
    else if (md >= 1123 & md <= 1222)
      rt = "射手座"
    else if ((md >= 1223 & md <= 1231) | (md >= 101 & md <= 119))
      rt = "摩蝎座"
    else
      rt = "未知"
  }
  rt
}

sqlContext.udf.register("constellation",  (x:String) => myfun(x))
var result = sqlContext.sql("SELECT constellation(birthday), count(constellation(birthday)) FROM customer group by constellation(birthday)")
result.collect().foreach(println)

result = sqlContext.sql("SELECT gender, count(gender) FROM customer where gender = 'F' or gender = 'M' group by gender")
result.collect().foreach(println)
