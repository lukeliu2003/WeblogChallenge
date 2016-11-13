package com.paytmlabs.weblogchallenge

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD


object Transform {
  
  def CSVReader(sparkSQL: SQLContext, input: String) : DataFrame = {  
    
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
    
    return inputDF
  }
  
  def Sessionization(sparkSQL: SQLContext, inputDF: DataFrame, sessionInterval: Int) : DataFrame = {
        
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
          sessionID = 0
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
    
    val sessionizedDF = sparkSQL.createDataFrame(sessionizedRDD, parsedDF.schema)
    return sessionizedDF
  }
}