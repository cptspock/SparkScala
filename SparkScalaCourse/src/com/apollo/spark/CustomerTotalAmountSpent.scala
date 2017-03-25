package com.apollo.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

object CustomerTotalAmountSpent {
  
  def parseLine(line : String) = {
    val fields = line.split(",")
    val customerId = fields(0)
    val amount = fields(2).toFloat
    (customerId, amount)
  }
  
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkContext = new SparkContext("local[*]", "CustomerTotalAmount")
    val lines = sparkContext.textFile("../customer-orders.csv")
    
    val parsedLines = lines.map(parseLine)
    val amounts = parsedLines.reduceByKey((x,y) => (x+y))
    
    val sortedAmounts = amounts.map(x => (x._2, x._1)).sortByKey(false)
    val output = sortedAmounts.collect()
    output.foreach(println)
  }
    
}