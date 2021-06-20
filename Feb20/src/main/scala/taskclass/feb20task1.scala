package taskclass

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object feb20task1 {
  
  
   def main (args:Array[String]) : Unit={
    
    	println("hello World")

			val conf = new 
			SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
sc.setLogLevel("ERROR")
   
val spark= SparkSession.builder().getOrCreate()   
   import spark.implicits._
   
   
   
   
   
   
 
   
   
   
   
   
   val data = spark.read.format("csv").load("file:///c:/spark/source data/usdata.csv")
   
   data.write.format("csv").option("delimiter","~").mode("overwrite").save("file:///c:/spark/final data/sepusdata")
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
//   val data = sc.textFile("file:///c:/spark/source data/usdata1.csv")
//   
//   
//    val struct =
//   StructType(
//     StructField("first_name", StringType, true) ::
//     StructField("last_name", StringType, false) ::
//     StructField("company_name", StringType, true)::
//     StructField("address", StringType, true)::
//     StructField("city", StringType, false)::
//     StructField("county", StringType, true)::
//     StructField("state", StringType, true)::
//     StructField("zip", StringType, true)::
//     StructField("age", StringType, false)::
//     StructField("phone1", StringType, true)::
//     StructField("email", StringType, true)::
//     StructField("web", StringType, false):: Nil) 
//   
//     
//     val data1 = data.mapPartitions(x=>x.drop(1))
//   
//    val split = data1.map(x=>x.split(",")).map(x=>Row(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
//       
//       val df = spark.createDataFrame(split,struct)
//       
//       df.show()
   
   
//   val data = sc.textFile("file:///c:/spark/source data/usdata.csv")
//   val header = data.first()
//   val data1 = data.filter(x=>x!=header)
//   
//  val struct =
//   StructType(
//     StructField("first_name", StringType, true) ::
//     StructField("last_name", StringType, false) ::
//     StructField("company_name", StringType, true)::
//     StructField("address", StringType, true)::
//     StructField("city", StringType, false)::
//     StructField("county", StringType, true)::
//     StructField("state", StringType, true)::
//     StructField("zip", StringType, true)::
//     StructField("age", StringType, false)::
//     StructField("phone1", StringType, true)::
//     StructField("email", StringType, true)::
//     StructField("web", StringType, false):: Nil) 
//   
//   val split = data1.map(x=>x.split(",")).map(x=>Row(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
//       
//       val df = spark.createDataFrame(split,struct)
//       
//       df.show()
//       
       
       
       
       
   
   
   
   
   
   
   
//   val data = spark.read.format("csv").load("file:///c:/spark/source data/txns")
//   
//   data.coalesce(1).  write.mode("overwrite").format("com.databricks.spark.avro").save("file:///c:/spark/final data/avrotxns")
//   println("=========first run========")
//   
//   val dataj = spark.read.format("com.databricks.spark.avro").load("file:///c:/spark/final data/avrotxns")
//   
//   data.coalesce(1).write.mode("overwrite").format("json").save("file:///c:/spark/final data/jsontxns")
//   
//    println("=========second run========")
   
   }
}