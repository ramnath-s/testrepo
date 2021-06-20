package Sparkmapsession

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Sparkmapsession {
  case class schema(state:String,city:String)
  
  def main (args:Array [String]) :Unit ={
    
    
    println("Hello world")
      
     val conf = new 
    SparkConf().setAppName("first").setMaster("local[*]")
    	val sc = new SparkContext(conf)
    
    
    val malist = List("Telangana->Hyderabad,TamilNadu->Chennai,Andhra->Vizag,Kerala->Trivandrum,Telangana->Kammam")
    
    val flatlist = malist.flatMap(x=>x.split(","))
    flatlist.foreach(println)
    
        flatlist.foreach(println)
     val maplist = flatlist.map(x=>x.split("->"))
     val schemalist = maplist.map(x=>schema(x(0),x(1)))
     
     val filtercity = schemalist.filter(x=>x.state.contains("Telangana"))
     val fil2 = filtercity.map(x=>x.city)
     fil2.foreach(println)
   }
    
  
}