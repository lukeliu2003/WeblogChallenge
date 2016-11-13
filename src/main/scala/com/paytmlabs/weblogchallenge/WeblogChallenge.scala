package com.paytmlabs.weblogchallenge

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe


object WeblogChallenge {
  
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }
  
  def main(args : Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: Sessionize input outputDir interval")
      return
    }
    
    val input : String = args(0)
    val outputDir : String  = args(1)
    // Default session interval is 15 minutes, and convert it into seconds
    val sessionInterval: Int = toInt(args(2)).getOrElse(15) * 60
    
    val conf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
    
    val spark = new SparkContext(conf)
    val sparkSQL = new SQLContext(spark)
    
    /*
     * Sessionize the web log by IP
     */
    
    // Load log data
    val inputDF = Transform.CSVReader(sparkSQL, input)
    
    // Extract columns we needed
    // Partition by ip and ordered by ip and timestamp
    // Sessionize
    val sessionizedDF = Transform.Sessionization(sparkSQL, inputDF, sessionInterval)
    // Cache
    sessionizedDF.persist()    
    // Output sessionized log data as parquet files
    sessionizedDF.write.mode(SaveMode.Overwrite).parquet(outputDir)
    
     /*
     * Analyze sessionized log data
     */
    
    val sessionizedRDD = sessionizedDF.rdd
    // Session time in seconds
    val sessiontime = Analyze.SessionTime(sessionizedRDD)
    // Cache
    sessiontime.persist()
    
    /*
     * Example 1: Determine the average session time
     */
    // Average session time in seconds
    val avg_sessiontime : Int = (sessiontime.values.sum() / sessiontime.values.count()).toInt
    
    /*
     * Example 2: Find the most engaged users, ie the IPs with the longest session times
     */
    // Most engaged users
    val engaged_user_sample = Analyze.MostEngagedUser(sessiontime).take(100)
    
    /*
     * Example 3: Determine unique URL visits per session
     */
    // Unique URL visits per session
    val uniqueurl_sample = Analyze.UniqueURLVisit(sessionizedRDD).take(100)
    
    // Print out to console
    println("Average session times in seconds")
    println(avg_sessiontime)
    
    println("Most engaged users, ie the IPs with the longest session times in seconds")
    engaged_user_sample.foreach(println)
    
    println("Determine unique URL visits per session")
    uniqueurl_sample.foreach(println)
    
    spark.stop()
  }
}
