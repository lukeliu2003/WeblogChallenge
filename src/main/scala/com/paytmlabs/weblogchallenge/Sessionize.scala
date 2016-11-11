package com.paytmlabs.weblogchallenge

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe


object Sessionize {
  
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
    
    // Create schema to read log file
    val logSchema = StructType(Array(
      StructField("timestamp", TimestampType, true),
      StructField("elb", StringType, true),
      StructField("client_port", StringType, true),
      StructField("backend_port", StringType, true),
      StructField("request_processing_time", FloatType, true),
      StructField("backend_processing_time", FloatType, true),
      StructField("response_processing_time", FloatType, true),
      StructField("elb_status_code", StringType, true),
      StructField("backend_status_code", StringType, true),
      StructField("received_bytes", IntegerType, true),
      StructField("sent_bytes", IntegerType, true),
      StructField("request", StringType, true),
      StructField("user_agent", StringType, true),
      StructField("ssl_cipher", StringType, true),
      StructField("ssl_protocol", StringType, true)))
    
    // Read and parse log line using databricks spark-csv
    val inputDF = sparkSQL.read
      .format("com.databricks.spark.csv")
      .option("delimiter", " ")
      .option("quote", "\"")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
      .schema(logSchema)
      .load(input)
    
    // Extract IP without port number
    val extractIP: (String => String) = (client_port: String) => {
      val ipRegex = """.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*""".r
      val ipRegex(ip) = client_port
      ip
    }
    // Extract URL path
    val extractURL: (String => String) = (request: String) => {
      val urlRegex = """.*?(https?://[^? ]+).*""".r
      val urlRegex(url) = request
      url
    }
    // Initialize session id as none
    val initSessionID: (Long => Option[Long]) = (_: Long) => {
      None
    }
    val extract_ip = udf(extractIP)
    val extract_url = udf(extractURL)
    val init_session_id = udf(initSessionID)
    
    // Transform and extract columns we needed
    // Partitioned by ip and ordered by ip and timestamp
    val parsedDF = inputDF
    .withColumn("access_epoch", unix_timestamp(col("timestamp"))) // unixtime in seconds
    .withColumn("client_ip", extract_ip(col("client_port")))
    .withColumn("access_url", extract_url(col("request")))
    .withColumn("session_id", init_session_id(col("access_epoch")))
    .repartition(col("client_ip"))
    .orderBy("client_ip", "access_epoch")
    .select("client_ip", "access_epoch", "access_url", "session_id")
    
    // Do window analysis to sessionize log line
    val sessionizedRDD = parsedDF.mapPartitions(itr => {
      val results = new scala.collection.mutable.MutableList[Row]
      var lastClientIP = "0.0.0.0"
      var lastRow : Row = null
      var sessionID: Long = 0
      
      // Loop through each row in the partition
      // and compare timestamp with previous row to do the window analysis
      itr.foreach(row => {
        val clientIP = row.getString(0)
        val accessEpoch = row.getLong(1)
        val accessURL = row.getString(2)
        
        if (!clientIP.equals(lastClientIP)) {
          lastClientIP = clientIP
          lastRow = null
        } else {
          if (lastRow != null) {
            if (accessEpoch - lastRow.getLong(1) > sessionInterval) {
              sessionID += 1
            }
          }
        }
        
        lastRow = Row(clientIP, accessEpoch, accessURL, sessionID)
        results += lastRow
      })
      results.iterator
    })
    
    sessionizedRDD.persist()
    
    // Output sessionized log data as parquet files
    val sessionizedDF = sparkSQL.createDataFrame(sessionizedRDD, parsedDF.schema)
    sessionizedDF.write.mode(SaveMode.Overwrite).parquet(outputDir)
    
    /*
     * Analyze sessionized log data
     */
    
    // Session time in seconds
    val sessiontime = sessionizedRDD
      .map { case Row(client_ip: String, access_epoch: Long, access_url: String, session_id: Long) => ((client_ip, session_id), (access_epoch, access_epoch)) }
      .reduceByKey((ae1, ae2) => (Math.max(ae1._1, ae2._1), Math.min(ae1._2, ae2._2)))
      .map({ case (key, value) => (key, value._1 - value._2 )})
    
    sessiontime.persist()
    
    /*
     * Example 1: Determine the average session time
     */
    // Average session time in seconds
    val avg_sessiontime : Int = (sessiontime.values.sum() / sessiontime.values.count()).toInt
    
    /*
     * Example 2: Find the most engaged users, ie the IPs with the longest session times
     */
    // Longest session time in seconds
    val max_sessiontime = sessiontime
      .max()(new Ordering[Tuple2[Tuple2[String, Long], Long]]() {
               override def compare(x: ((String, Long), Long), y: ((String, Long), Long)): Int = Ordering[Long].compare(x._2, y._2)
             })._2
    // Most engaged users
    val engaged_user_sample = sessiontime
      .filter { case (key, value) => value == max_sessiontime }
      .take(100)
    
    /*
     * Example 3: Determine unique URL visits per session
     */
    // Unique URL visits per session
    val uniqueurl_sample = sessionizedRDD
      .map { case Row(client_ip: String, access_epoch: Long, access_url: String, session_id: Long) => ((client_ip, session_id), (access_url, 1)) }
      .reduceByKey((au1, au2) => (null, if (au1._1 == au2._1) au1._2 else au1._2 + au2._2 ))
      .map({ case (key, value) => (key, value._2) })
      .take(100)
    
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
