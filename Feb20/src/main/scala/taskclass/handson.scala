package taskclass

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object handson {
  
  
  def main (args:Array[String]) : Unit={
    
    	println("hello World")

			val conf = new 
			SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
sc.setLogLevel("ERROR")
   val spark= SparkSession.builder().getOrCreate()   
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
													StructField("spendby", StringType, false)::Nil)
													

			
			val pardata = spark.read.schema(struct).format("csv").load("file:///c:/spark/source data/txns")
			
			pardata.coalesce(1).write.format("parquet").mode("overwrite").save("file:///c:/spark/final data/txns.parquet")
			
			val jasdata = spark.read.schema(struct).format("parquet").load("file:///c:/spark/final data/txns.parquet")
			
			jasdata.coalesce(1).write.format("json").mode("overwrite").save("file:///c:/spark/final data/txns.json")
		
			val orcdata =spark.read.schema(struct).format("json").load("file:///c:/spark/final data/txns.json")
			
		 orcdata.coalesce(1).write.format("orc").mode("overwrite").save("file:///c:/spark/final data/txnsfinal.orc")
			  
  val csvdata = spark.read.schema(struct).format("orc").load("file:///c:/spark/final data/txnsfinal.orc")
   
   csvdata.coalesce(1).write.format("csv").mode("overwrite").save("file:///c:/spark/final data/txnsfinal.csv")
			
    
    
  }
}