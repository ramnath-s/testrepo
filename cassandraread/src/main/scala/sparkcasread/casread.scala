package sparkcasread

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.sql.cassandra._
import com.databricks.spark.avro

object casread {
  
  
    def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("data").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("Error")

			val spark = SparkSession.builder()
			.config("spark.cassandra.connection.host","localhost")
			.config("spark.cassandra.connection.port","9042")
			.getOrCreate()

			import spark.implicits._
  
  val txn_cassandra_df = spark.read.format("org.apache.spark.sql.cassandra")
			.option("keyspace","zeyobron27")
			.option("table","txnrecords")
			.load()
			
			txn_cassandra_df.show()
			println("---------s1---")
			
			txn_cassandra_df.write.format("org.apache.spark.sql.cassandra")
		.option("keyspace","zeyobron27")
			.option("table","txnrecords_write")
			.mode("append")
			.save()
			println("---------s2---")
			val txn_cassandra_df1 = spark.read.format("org.apache.spark.sql.cassandra")
			.option("keyspace","zeyobron27")
			.option("table","txnrecords_write")
			.load()
			
		println("---------show	txn_cassandra_df1---")
			txn_cassandra_df1.show()
	println("---------show	txn_cassandra_df---")
			txn_cassandra_df.show()
			
				println("---------creating union---")
			
  val uniondf = txn_cassandra_df.union(txn_cassandra_df1)
  println("---------show union---")
  uniondf.show()
  
  uniondf.write.format("com.databricks.spark.avro")
  .save("file:///C://spark//final data/caswrite")
  
  
  
    }
  
  
}