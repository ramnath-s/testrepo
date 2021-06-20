package HAndson

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml
import org.apache.spark.sql.functions._
import com.databricks.spark.avro
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.execution.datasources.hbase._




object tasks {
  
  def main (args: Array[String]):Unit={
  
      val conf = new SparkConf().setAppName("First").setMaster("local[*]")
  
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
 
    
    val data = spark.read.format("csv")
    .option("header","true")
//     .load("hdfs:////user/cloudera/prj3/sample table.txt")
      .load("file:///c:/spark/source data/prj3/sample table.txt")
    
     data.show()
     
     val datalist1 = data.select(concat(col("hcolumn"),lit(","),col("scolumn")))
     .rdd.collect()
     
     val h = data.select("hcolumn").collect().toList
       val h1 = data.select("hcolumn").collect()
       val s = data.select("scolumn").collect().toList
     val s2 = data.select("scolumn").collect()  
     val datalist = data.rdd.collect
     datalist.foreach(println)
     
  
   
val a = h.length

println(a)
     
   
     
    val firstv = """{
    |"table":{"namespace":"default", "name":"hbase_tract10"},
    |"rowkey":"rowkey",
    |"columns":{
    |"masterid":{"cf":"rowkey", "col":"rowkey", "type":"string""""+ System.lineSeparator()
       
    var secondvar = ""
    
      for (second <- datalist)
      { var hivecol:String = "\"" + second.getString(1) +"\":{\"cf\":\"cf\""+", \"col\":"+second.getString(0) + ", \"type\":\"string\"}," + System.lineSeparator()
        
         secondvar = secondvar + hivecol
      }
      
    secondvar = secondvar.trim().dropRight(1)
    
 val thirdvar  = System.lineSeparator()+"""}"""+System.lineSeparator() 
     
          var cata:String = (firstv + secondvar+ thirdvar).stripMargin
     
          println("--------phase 1-----------")
          
        println(cata)
 
        val hbasedf = spark.read.options(Map(HBaseTableCatalog.tableCatalog-> cata))
        .format("org.apache.spark.sql.execution.datasource.hbase")
        .load()
        
        hbasedf.show()
        
    val prelist = data.select("scolumn").rdd.map(x=>(x(0).toString().split("_")(0))).collect.toList.distinct
    
    println(prelist)
     
  }
    
}