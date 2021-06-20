package tasks
import org.apache.spark.SparkConf
 import org.apache.spark.SparkContext

object task1 {
    case class schema(name:String,state:String,age:Int)
  def main(args:Array[String]):Unit={
    
    println("hello World")
    
    val conf = new 
    SparkConf().setAppName("first").setMaster("local[*]")
    	val sc = new SparkContext(conf)
    
    
    val lista = List("Sai,Andhra,40~Aditya,Telangana,50~Haasya,TamilNadu,20~Manju,Andhra,50~Geetha,TamilNadu,70~Ravi,Andhra,20~Satya,TamilNadu,30")
    
    val flatlista = lista.flatMap(x=>x.split("~"))
    
    val splitlis = flatlista.map(x=>x.split(","))
    
    val schemlis = splitlis.map(x=>schema(x(0),x(1),x(2).toInt))
    
    val filterlis = schemlis.filter(x=>x.state.contains("TamilNadu") && x.age>30)
    
    val name = filterlis.map(x=>x.name)
    
    name.foreach(println)
         
    
    
     
  }

  
}