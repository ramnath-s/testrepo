package handson

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object tasks {
  
  
  def main (args:Array[String]) : Unit={
    
    	println("hello World")
    	
    			val conf = new 
			SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")
   
val spark= SparkSession.builder().getOrCreate()   
   import spark.implicits._
   
  val df = spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///home/cloudera/data/book.xml")
    df.show()

df.coalesce(1).write.mode("overwrite").format("json").option("rowTag","book").save("hdfs:///user/cloudera/bookdata7")
    println("done")
  
  
  }
  
  
  
  
}