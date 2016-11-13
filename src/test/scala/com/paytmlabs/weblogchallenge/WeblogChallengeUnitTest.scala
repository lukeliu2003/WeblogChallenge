package com.paytmlabs.weblogchallenge

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


class WeblogChallengeUnitTest extends FlatSpec with Matchers with BeforeAndAfter {
  
  var spark: SparkContext = _
  var sparkSQL: SQLContext = _
  var inputDF : DataFrame = _
  var sessionizedRDD : RDD[Row] = _
  var sessiontime : RDD[((String, Long), Long)] = _
  
  before {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WeblogChallengeUnitTest")
    spark = new SparkContext(conf)
    sparkSQL = new SQLContext(spark)
    
    val input = getClass.getResource("/test.log").getPath
    inputDF = Transform.CSVReader(sparkSQL, input)
    
    val sessionInterval: Int = 15 * 60
    sessionizedRDD = Transform.Sessionization(sparkSQL, inputDF, sessionInterval).rdd
    
    sessiontime = Analyze.SessionTime(sessionizedRDD)
  }
  
  after {
    spark.stop()
  }
  
  behavior of "CSV Reader"
  
  it should "Read CSV file into Dataframe" in {
    inputDF.count() should equal (6)
  }
  
  behavior of "Sessionization"

  it should "Add session id to each row" in {
    sessionizedRDD.collect should contain allOf (
      Row("101.221.131.147",1437592124,"https://paytm.com:443/shop/paytmcashConfirmation",0), 
      Row("148.251.185.126",1437592534,"https://paytm.com:443/shop",0), 
      Row("220.226.206.7",1437592140,"https://paytm.com:443/recharges",0), 
      Row("220.226.206.7",1437592344,"https://paytm.com:443/recharges",0), 
      Row("220.226.206.7",1437592352,"https://paytm.com:443/recharges",0), 
      Row("220.226.206.7",1437592358,"https://paytm.com:443/recharges",0)
    )
  }
  
  behavior of "Session time in seconds"
  
  it should "Calculate session time per user session" in {
    sessiontime.collect should contain allOf (
      (("220.226.206.7",0),218), 
      (("101.221.131.147",0),0), 
      (("148.251.185.126",0),0)
    )
  }
  
  behavior of "Average session time in seconds"
  
  it should "Calculate average session time of all sessions" in {
    val avg_sessiontime : Int = Analyze.AverageSessionTime(sessiontime)
    
    avg_sessiontime should equal (72)
  }

  behavior of "Most engaged users"

  it should "Find most engaged users with longest session times" in {
    val engaged_user_sample = Analyze.MostEngagedUser(sessiontime)
    
    engaged_user_sample.collect should equal (
      Array((("220.226.206.7",0),218))
    )
  }
  
  behavior of "Unique URL visits per session"
  
  it should "Find unique URL visits per session" in {
    val uniqueurl_sample = Analyze.UniqueURLVisit(sessionizedRDD)
    
    uniqueurl_sample.collect should contain allOf (
      (("220.226.206.7",0),3), 
      (("101.221.131.147",0),1), 
      (("148.251.185.126",0),1)
    )
  }
}