package Tasks

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.databricks.spark.avro
import com.databricks.spark.xml

case class schema (txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String,country:String)

object Tasks {
  
  def main (args:Array[String]): Unit ={
 
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
     StructField("spendby", StringType, false)::
     StructField("country", StringType, true):: Nil) 
    
    
    
    println ("hello world")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = sc.textFile(args(0))
    val flatdata = data.flatMap(x=>x.split("~"))
   flatdata.take(5).foreach(println)
    
    println()
    
   val header = flatdata.first()
   val headerless = flatdata.filter(x=>x!=header)
    headerless.take(5).foreach(println)
       println()
    
    val filterdata = headerless.filter(x=>x.contains("Gymnastics") && x.contains("cash"))
      filterdata.take(5).foreach(println)
     println()
    val usdata = filterdata.map(x=>x.+(",").+("US_Txns"))
    usdata.take(5).foreach(println)
    
    val splitda = usdata.map(x=>x.split(","))
    splitda.take(5).foreach(println)
    
    val spark= SparkSession.builder().getOrCreate()   
   import spark.implicits._
    
    val schemardd = splitda.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
    val filters =schemardd.filter(x=>x.txnno.toInt>50000)
    x`
   df.show()

   df.coalesce(1).write.format("json").mode("overwrite").save("hdfs:///user/cloudera/finaldata/usbaddatajson")
   df.coalesce(1).write.format("parquet").mode("overwrite").save("hdfs:///user/cloudera/finaldata/usbaddataparquet")
   
   
     val row = splitda.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
    val rowdf = spark.createDataFrame(row, struct)
   rowdf.createOrReplaceTempView("tempdf")
   val rodff = spark.sql("select * from tempdf where txnno>50000")
   rodff.show()
   
   rodff.coalesce(1).write.format("orc").mode("overwrite").save("hdfs:///user/cloudera/finaldata/usbaddataorc")
   rodff.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("hdfs:///user/cloudera/finaldata/usbaddataavro")
   
   println("======written=========")
   
   
   
   
   
   
   
   
    
  }
  
}