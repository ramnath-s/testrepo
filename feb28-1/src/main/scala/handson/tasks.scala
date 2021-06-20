package handson

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object tasks {
  
    
  def main (args:Array[String]):Unit= {
 
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
      val sc =  new SparkContext(conf)
      sc.setLogLevel("ERROR")
    
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
    
         val struct =
   StructType(
     StructField("txnno", StringType, true) ::
     StructField("txndate", StringType, false) ::
     StructField("custno", StringType, true)::
     StructField("amount", StringType, true)::
     StructField("category", StringType, false)::
     StructField("product", StringType, true)::
     StructField("city", StringType, true)::
     StructField("state", StringType, true)::
     StructField("spendby", StringType, false):: Nil) 
      
      
      
      val data = sc.textFile("file:///home/cloudera/data/txns")
  val splitda = data.map(x=>x.split(","))
   splitda.take(5).foreach(println)
      val rowrdd = splitda.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
     val rowdf = spark.createDataFrame(rowrdd, struct)
      rowdf.show()
      
      rowdf.createOrReplaceTempView("tempdf") 
      
      val exer = spark.sql("select * from tempdf where category = 'Exercise & Fitness'")
      exer.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("hdfs:///user/cloudera/finaldata/Exdata")
      exer.show()
      
      
      
      val udata = spark.read.format("csv").option("header","true").load("file:///home/cloudera/data/usdata.csv")
      udata.coalesce(1).write.format("parquet").option("compression","gzip").mode("overwrite").save("hdfs:///user/cloudera/finaldata/uspar")
      udata.show()
      
      
val jdata = spark.read.format("json").load("file:///home/cloudera/data/devices.json")
 jdata.coalesce(1).write.format("com.databricks.spark.xml").option("rootTag","devices").option("rowTag","fields").save("hdfs:///user/cloudera/finaldata/usxml")
 jdata.show()    
 
 
  }
  
}