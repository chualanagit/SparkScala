package alan.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._


object PopularPeople {
  
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  def main(args: Array[String]) {

    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "PopularPeople", Seconds(1))
    
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    
    val statuses = tweets.map(status => status.getText())
    
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    val people = tweetwords.filter(word => word.startsWith("@") && word != "@")
    
    val peopleKeyValues = people.map(tag => (tag, 1))
    
    val PeopleCount = peopleKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(180), Seconds(1))

    val sortedResults = PeopleCount.transform(rdd => rdd.sortBy(x => x._2, false))
    
    sortedResults.print
    
    ssc.checkpoint("~/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}