package com.streaming.twitter

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.cassandra
import com.datastax.spark.connector.streaming._


object MatchStream {

   val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming")
    // here local[*] shows that spark code will run on all possible cores.
    val ssc = new StreamingContext(conf, Seconds(10))
    
    var consumerKey = ""
    var consumerSecret = ""
    var accessToken = ""
    var accessTokenSecret = ""
    
    val keyFile = "/home/omkar/Desktop/twitterSetup"
    val line = Source.fromFile(keyFile).getLines()
    
    for(line <- Source.fromFile(keyFile).getLines()){
       val keys = line.split(",")
       consumerKey = keys(0).toString()
       consumerSecret = keys(1).toString()
       accessToken = keys(2).toString()
       accessTokenSecret = keys(3).toString()
    }
   
    def tweetConversionFunction(tweets : Status) : (String, String, String, String) = {
       
             var name = tweets.getUser.getName
             var location = tweets.getUser.getLocation
             val lang = tweets.getLang 
             val hashtag = tweets.getText.split(" ").filter(_.startsWith("#")).toString()
//             val tweet = tweets.getText
//             val latitude = tweets.getGeoLocation.getLatitude
//             val longitude = tweets.getGeoLocation.getLongitude
             
             if(location.equals(null)){
                location = "default"
             }
             if(name.equals(null)){
               name = "default"
             }
//             here name and location cant be null because we are having location and name
//             as a part of primary key in cassandra
             (location, name, hashtag, lang)
       
     }
    
  def main(args: Array[String]){
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    val filters = Array("IndiavPakistan", "nidiavpak", "pakistanvindia", "pakvindia", "cricket",
                        "indiavspakistan", "worldcup", "world cup 2019", "INDvPAK", "PAKvIND")
    
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    
    val tweetConversion = tweets.map(tweetConversionFunction)
    
    tweetConversion.saveToCassandra("streaming", "indiavpak")
    
//    tweetConversion.print()
      
    ssc.start()
    ssc.awaitTermination()
    
  }
  
 
}