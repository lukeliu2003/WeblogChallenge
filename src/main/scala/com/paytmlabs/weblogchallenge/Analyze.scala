package com.paytmlabs.weblogchallenge

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD


object Analyze {
  
  def SessionTime(sessionizedRDD: RDD[Row]) : RDD[((String, Long), Long)] = {
    // Session time in seconds
    val sessiontime = sessionizedRDD
      .map { case Row(client_ip: String, access_epoch: Long, access_url: String, session_id: Long) => ((client_ip, session_id), (access_epoch, access_epoch)) }
      .reduceByKey((ae1, ae2) => (Math.max(ae1._1, ae2._1), Math.min(ae1._2, ae2._2)))
      .map({ case (key, value) => (key, value._1 - value._2 )})
    
    return sessiontime
  }
  
  def AverageSessionTime(sessiontime: RDD[((String, Long), Long)]) : Int = {
    // Average session time in seconds
    val avg_sessiontime : Int = (sessiontime.values.sum() / sessiontime.values.count()).toInt
    
    return avg_sessiontime
  }
  
  def MostEngagedUser(sessiontime: RDD[((String, Long), Long)]) : RDD[((String, Long), Long)] = {
    // Longest session time in seconds
    val max_sessiontime = sessiontime
      .max()(new Ordering[Tuple2[Tuple2[String, Long], Long]]() {
               override def compare(x: ((String, Long), Long), y: ((String, Long), Long)): Int = Ordering[Long].compare(x._2, y._2)
             })._2
    
    // Most engaged users
    val engaged_user = sessiontime.filter { case (key, value) => value == max_sessiontime }
    
    return engaged_user
  }
  
  def UniqueURLVisit(sessionizedRDD: RDD[Row]) : RDD[((String, Long), Long)] = {
    // Unique URL visits per session
    val uniqueurl_sample = sessionizedRDD
      .map { case Row(client_ip: String, access_epoch: Long, access_url: String, session_id: Long) => ((client_ip, session_id), (access_url, 1L)) }
      .reduceByKey((au1, au2) => (null, if (au1._1 == au2._1) au1._2 else au1._2 + au2._2 ))
      .map({ case (key, value) => (key, value._2) })
    
    return uniqueurl_sample
  }
}