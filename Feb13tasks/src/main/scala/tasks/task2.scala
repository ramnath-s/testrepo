package tasks

 import org.apache.spark.SparkConf
 import org.apache.spark.SparkContext
 

object task2 {
  
   case class schema(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String
)
  def main(args:Array[String]):Unit={
    
    println("hello World")
    
    val conf = new 
    SparkConf().setAppName("first").setMaster("local[*]")
    	val sc = new SparkContext(conf)
    
    
    println("Task 2")
    
    val data = sc.textFile("file:///c:/spark/source data/txns")
    
    val dataset =data.map(x=>x.split(","))
    
    val schemdata =dataset.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
        
    val filterdata =schemdata.filter(x=>x.category.contains("Gymnastics"))
    
    val taxno = filterdata.map(x=>x.txnno)
    
    taxno.saveAsTextFile("file:///c:/spark/final data/txnno") 
      
     
  }
  
}