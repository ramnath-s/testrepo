package tasks
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object classtasks {
  
  
  def main (args:Array[String]) :Unit ={
    
    val conf = new 
    SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val dataset = sc.textFile("file:///c:/spark/source data/usdata.csv")
    
    val lendata = dataset.filter(x=>x.length()>200)
    
    val flatdata = lendata.flatMap(x=>x.split(","))
    
    val condata = flatdata.map(x=>x+(", Zeyo"))
    
     condata.foreach(println)
    
    val flatdata2 = condata.flatMap(x=>x.split("-"))
     
     flatdata2.foreach(println)
     
     val dou = flatdata2.map(x=>x.replace("\"",""))
         
         dou.foreach(println)
         
        val space = dou.map(x=>x.replace(" ","")) 
         
        space.foreach(println)
        
        space.saveAsTextFile("file:///c:/spark/final data/space") 
         
    
    
  }
}