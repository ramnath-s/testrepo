package tasks

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object feb14tasks {
  
  def main(args:Array[String]):Unit={
    
    println("hello World")
    
    val conf = new 
    SparkConf().setAppName("first").setMaster("local[*]")
    	val sc = new SparkContext(conf)
    
    val data1 = sc.textFile("file:///c:/spark/source data/rddj1")
    
    data1.foreach(println)
    
    println
    
    val data2 = sc.textFile("file:///c:/spark/source data/rddj2")
    data2.foreach(println)
    
    
    val key1 = data1.map(x=>x.split(",")).map(x=>(x(0),x(1)))
    
    key1.foreach(println)
    
    val key2 = data2.map(x=>x.split(",")).map(x=>(x(0),x(1)))
  
    key2.foreach(println)
    
    val joinrdd = key1.join(key2)
    
    val finrdd = joinrdd.map(x=>x._1+","+x._2._1+","+x._2._2)
    
    finrdd.foreach(println)
    
    
    
    
    
  
  }
  
}