package Handson

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml
import org.apache.spark.sql.functions._
import com.databricks.spark.avro
object tasks {
  
  def main (args: Array[String]):Unit={
  
  
    
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
  
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
  
    
     val data = spark.read.format("json")
    .option("multiLine","true")
    .load("file:///c:/spark/source data/place.json")
    
     println("----------place read---------")
        data.show()
    data.printSchema()
    
    val flat1 = data.select(
        col("place"),
        col("user.name"),
        col("user.address.number").alias("address_number"),
       col("user.address.street").alias("address_street"),
       col("user.address.pin").alias("address_pin")
      
    )
    println("----------task1---------")
          flat1.show(false)
    flat1.printSchema()
    
    println("----------task 2---------")
    
    val back = flat1.select(
             
     col("place"),
               struct(
            struct(
                col("address_number").alias("number"),
                col("address_pin").alias("pin"),
                col("address_street").alias("street")
                
            ).alias("address"),
            col("name")
        ).alias("user")
        )
    
        back.show(false)
    back.printSchema()
    
    
     val data1 = spark.read.format("json")
    .option("multiLine","true")
    .load("file:///c:/spark/source data/pets.json")
     println("----------pet read---------")
            data1.show(false)
    data1.printSchema()
    
 println("----------flatten pet read---------")
 val flat2 = data1.select(
     col("Name"),
     col("Mobile"),
     col("Address.*"),
     col("Pets")
     )
     flat2.show(false)
    flat2.printSchema()
 

 
    
//    data.show()
//    data.printSchema()
//    
//    val formatdf = data.selectExpr("id","image.height as image_height"
//        ,"image.url as image_url","image.width as image_width"
//        ,"name","thumbnail.height as thumb_height"
//        ,"thumbnail.url as thumb_url"
//        ,"thumbnail.width as thumb_width","type")
//        
//        
//          
//    formatdf.show()
//    formatdf.printSchema()
//        
//        
//        val jsonstruct = formatdf.select(
//            
//            col("id"),
//            struct(
//                
//               col("image_height").alias("height"),
//               col("image_url").alias("url"),
//               col("image_width").alias("width")
//                 
//            ).alias("image"),
//            
//            col("name"),
//            
//            struct(
//                
//               col("thumb_height").alias("height"),
//               col("thumb_url").alias("url"),
//               col("thumb_width").alias("width")
//                 
//            ).alias("thumbnail"),
//            col("type")
//                  )
//                
//    jsonstruct.show(false)
//    jsonstruct.printSchema()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
  
//    
//   val data1 = spark.read.format("json")
//    .option("multiLine","true")
//    .load("file:///c:/spark/source data/zeyo27_json.json") 
//     data1.show()
//    data1.printSchema()
//    
//    val form1df = data1.selectExpr("first_name","second_name as last_name","address.temporary_address.city as temp_city","address.temporary_address.state as temp_state") 
//        form1df.show()
//    form1df.printSchema()
    
    
    
    
    
    
//    first_name last_name temp_city temp_state
//    
    
    
     
//      
//     id image_height image_url image_width name thumb_height thumb_url thumb_width type
//    
//    val data = spark.read.format("json")
//    .option("multiLine","true")
//    .load("file:///c:/spark/source data/JSON sample.json")
//    
//    data.show()
//    data.printSchema()
//    
//    val flatdf = data.select("*","address.permanentaddress","address.temporaryaddress").drop("address")
//
//   val flatdf1 = data.selectExpr("first_name","second_name","address.permanentaddress.city as peradd_city")
//
//   flatdf.show()
//    flatdf.printSchema()
//    flatdf1.show()
//    flatdf1.printSchema()
   
  }
  
}