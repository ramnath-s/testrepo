package useranalytics


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml
import org.apache.spark.sql.functions._
import com.databricks.spark.avro
import org.apache.spark.sql.hive.HiveContext

object useranalytics {
  def main (args: Array[String]):Unit={
    
     val conf = new SparkConf().setAppName("First").setMaster("local[*]")
  
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    
    val spark = SparkSession.builder().enableHiveSupport()
    .config("hive.exec.dynamic.partition.mode","nonstrict")
    .getOrCreate()
    import spark.implicits._
    val hc = new HiveContext(sc)
import hc.implicits._
    println("Hello World")
     
      val date = java.time.LocalDate.now.minusDays(1).toString
      val parts = date.split("-")
     
 val year = parts(0)
 val month = parts(1)   
 val day = parts(2)
     
     val df1 = spark.read.format("com.databricks.spark.avro")
     .load(s"hdfs:////user/cloudera/proj2/$year/$month/$day/logfile.avro")

     
     
     println("-------step 2 read file 1---------")
     df1.show(5)
     df1.printSchema()
     
     
     val url = "https://randomuser.me/api/0.8/?results=1000"
  val data = scala.io.Source.fromURL(url).mkString
//  println(data)
     
      val df2r= sc.parallelize(List(data))
      
//      df2r.foreach(println)
      
      val urd = df2r.coalesce(1).saveAsTextFile(s"hdfs:////user/cloudera/proj2/s2/$date")
      
      val df2 = spark.read.format("json")
      .option("multiLine","true")
      .load(s"hdfs:////user/cloudera/proj2/s2/$date")
      
      println("------- step 3 read file 2---------")
      df2.show(5)
      df2.printSchema()
      
      
       
    val df2a = df2.withColumn("results",explode(col("results")))
     .select("*","results.*").drop("results")
    .select("*","user.*").drop("user")
         .select("*","location.*").drop("location")
    .select("*","name.*").drop("name")
    .select("*","picture.*").drop("picture")
    println("------- step 4 file 2 flatten---------")
    df2a.show(5)
    df2a.printSchema()
    
    val df2b = df2a.withColumn("username" ,regexp_replace(col("username"),"[0-9]",""))
println("-------step 5 Remove the numericals---------")
df2b.show(5)
    
  val df3 =df2b.select("username","*")
    
       
  val df4 = df1.join(broadcast(df3),Seq("username"),"left")
  
println("-------step 6 Joined DF---------")
    
  df4.show(5)
  
  val df5 = df4.filter(col("nationality").isNull)
  println("-------7a Null DF---------")
  df5.show(5)
  
  val df6 = df4.filter(col("nationality").isNotNull)
  println("-------7b Not Null DF---------")
    df6.show(5)
  
  
   val df7a = df5.na.fill(0)
   val df7b = df7a.na.fill("Not Available")
  println("-------step 8 fill Null with NA and 0---------")
  df7b.show(5)
  
  val df8 = df6.withColumn("current_date",current_date())
    val df9 = df7b.withColumn("current_date",current_date())
  println("-------dates added 9a , 9b---------")
  df8.show(5)
  df9.show(5)
  
  df8.coalesce(1).write.partitionBy("current_date").mode("overwrite").format("parquet")
  .save("hdfs:////user/cloudera/proj2/nonull")
  
  df9.coalesce(1).write.partitionBy("current_date").mode("overwrite").format("parquet")
  .save("hdfs:////user/cloudera/proj2/null")
  println("------- step 10 Both files written---------")
  }
  
}