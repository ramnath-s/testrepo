package tasks

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object feb14task2 {

	def main(args:Array[String]):Unit= {

			println("hello World")

			val conf = new 
			SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)

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

 
    val data = sc.textFile("file:///c:/spark/source data/txns")
    
    val splitdata = data.map(x=>x.split(","))
    
    val rowdd = splitdata.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
        
    val dataframe = spark.createDataFrame(rowdd,struct)
    
      dataframe.show()
      
     dataframe.createOrReplaceTempView("temp")

     val tsport = spark.sql("select* from temp where category = 'Team Sports' and spendby = 'credit'")
     tsport.show()
													
			val convertRdd = tsport.rdd.map(_.mkString(","))
			
			convertRdd.take(10).foreach(println)
			convertRdd.saveAsTextFile("file:///c:/spark/final data/convert")
			
			println("======saved====")
													
	}


}