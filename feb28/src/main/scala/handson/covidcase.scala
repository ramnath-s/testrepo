package handson

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType}


case class schema (Direction:String,Year:String,Date:String,Weekday:String,Current_Match:String,Country:String,Commodity:String,Transport_Mode:String,Measure:String,Value:String,Cumulative:String)

object tasks {
  
  
  def main (args:Array[String]):Unit= {
 
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
      val sc =  new SparkContext(conf)
      sc.setLogLevel("ERROR")
    
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      

      val data = sc.textFile("file:///c:/spark/source data/effects-of-covid-19-on-trade-1-february-20-may-2020-provisional-csv.txt")
    
      val structschema = sc.textFile("file:///c:/spark/source data/schema_file.txt")
   val head = structschema.first()
      
      val schemaread = StructType(head.split(",").map(x ⇒ StructField(x, StringType, true)))
  
      println("schema")
      
      schemaread.foreach(println)
      
       val header = data.first()
   val datahdrem= data.filter(x=>x!=header)
   
      datahdrem.take(5).foreach(println)
    
       val splitda = datahdrem.map(x=>x.split(","))
    splitda.take(5).foreach(println)
    
   println("schemardd")
   
    val schemardd = splitda.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
    val tonndd = schemardd.filter(x=>x.Measure=="Tonnes")
      
    val tdf = tonndd.toDF()
    
    tdf.show()
    
     println("rowrdd")
    
   val rowrdd = splitda.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
     val rowdf = spark.createDataFrame(rowrdd, schemaread)
   rowdf.createOrReplaceTempView("tempdf") 

   val ddf = spark.sql("select * from tempdf where Measure = '$' ")
   
   ddf.show()
   
   println("task3")
   
   sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
   
   tdf.createOrReplaceTempView("tempr")
   
   val texport= spark.sql("select * from tempr where Direction = 'Exports'")
   texport.show()
   texport.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("file:///c:/spark/final data/Covidcase/covidcsv")
   
   ddf.createOrReplaceTempView("temprd")
   val dexport= spark.sql("select * from temprd where Direction = 'Exports'")
   dexport.coalesce(1).write.format("parquet").mode("overwrite").save("file:///c:/spark/final data/Covidcase/covidparquet")
   dexport.show()
   
    val dimport= spark.sql("select * from temprd where Direction = 'Imports'")
   dimport.coalesce(1).write.format("json").mode("overwrite").save("file:///c:/spark/final data/Covidcase/covidjson")
   dimport.show()
   
   
    val dreimports= spark.sql("select * from temprd where Direction = 'Reimports'")
   dreimports.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("file:///c:/spark/final data/Covidcase/covidavro")
   dreimports.show()
   
   val uniondf = dreimports.union(dimport).union(dexport).union(texport)
   uniondf.show()
   
   uniondf.coalesce(1).write.format("json").partitionBy("Direction").mode("overwrite").save("file:///c:/spark/final data/Covidcase/unionjson")
   uniondf.show()
   
   uniondf.coalesce(1).write.format("com.databricks.spark.xml").option("rootTag","covid_data").option("rowTag","report").mode("overwrite").save("file:///c:/spark/final data/Covidcase/unionxml")
   uniondf.show()
   
}
 
}