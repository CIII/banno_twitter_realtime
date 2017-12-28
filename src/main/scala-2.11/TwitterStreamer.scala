import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import com.netaporter.uri.Uri
import com.vdurmont.emoji.EmojiParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterStreamer {
  val tweetTotal = new AtomicLong(1)
  val tweetsWithEmojis = new AtomicLong(0)
  val tweetsWithDomains = new AtomicLong(0)
  val tweetsWithPhotoUrls = new AtomicLong(0)

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamer <consumer key> <consumer secret> <access token> <access token secret>")
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.ERROR)

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    sparkConf.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/tmp")

    val stream = TwitterUtils.createStream(ssc, None)
    val startTime = ssc.sparkContext.startTime

    stream.foreachRDD { rdd =>
      showTweetSummaryStats(rdd, startTime)
      showEmojiStats(rdd)
      showHashTagStats(rdd)
      showDomainStats(rdd)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def showTweetSummaryStats(rdd: RDD[Status], startTime: Long) = {
    tweetTotal.getAndAdd(rdd.count())
    val hourDiff = timeDiff(TimeUnit.HOURS, startTime)
    val minuteDiff = timeDiff(TimeUnit.MINUTES, startTime)
    val secondDiff = timeDiff(TimeUnit.SECONDS, startTime)
    println(s"Running tweet count: $tweetTotal")
    println(s"AVG tweets per hour: ${tweetTotal.get / (if(hourDiff == 0) 1L else hourDiff)}")
    println(s"AVG tweets per minute: ${tweetTotal.get / (if(minuteDiff == 0) 1L else minuteDiff)}")
    println(s"AVG tweets per seconds: ${tweetTotal.get / (if(secondDiff == 0) 1L else secondDiff)}")
  }

  def showEmojiStats(rdd: RDD[Status]) = {
    val emojis = rdd.flatMap { status =>
      val emojiArr = EmojiParser.extractEmojis(status.getText).toArray
      if (emojiArr.nonEmpty) tweetsWithEmojis.getAndAdd(1)
      println(s"Tweets with emojis: $tweetsWithEmojis")
      println(s"Percentage of tweets with emojis: ${getPercentage(tweetsWithEmojis.get, tweetTotal.get)}%")
      emojiArr
    }

    val topEmojiCounts60 = emojis.map((_, 1)).reduceByKey(_ + _).map{case (topic, count) => (count, topic)}

    val topList = topEmojiCounts60.take(10)
    println(s"Top 10 Emojis (${rdd.count()} total):")
    topList.foreach{case (count, emoji) => println(s"$emoji ($count tweets)")}
  }

  def showHashTagStats(rdd: RDD[Status]) = {
    val hashTags = rdd.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topHashTagsPerMinute = hashTags.map((_, 1)).reduceByKey(_ + _)
      .map{case (topic, count) => (count, topic)}

    val topList = topHashTagsPerMinute.take(10)
      println(s"Top 10 Hashtags (${rdd.count()} total):")
      topList.foreach{case (count, tag) => println(s"$tag ($count tweets)")}
  }

  def showDomainStats(rdd: RDD[Status]) = {
    val domains = rdd.flatMap { status =>
      val domainsArr = status.getText.replaceAll("\\P{Print}", "").split(" ").filter { frag =>
        (try { Uri.parse(frag) } catch { case e: Exception => Uri.empty }).host match {
          case Some(_) if frag.contains(".") => true
          case _ => false
        }
      }
      if (domainsArr.nonEmpty)
        tweetsWithDomains.getAndAdd(1)
      if(domainsArr.exists(d => d.contains("pic.twitter.com") || d.contains("instagram")))
        tweetsWithPhotoUrls.getAndAdd(1)

      println(s"Tweets with domains: $tweetsWithDomains")
      println(s"Percentage of tweets with domains: ${getPercentage(tweetsWithDomains.get, tweetTotal.get)}%")

      println(s"Tweets with photos: $tweetsWithPhotoUrls")
      println(s"Percentage of tweets with photos: ${getPercentage(tweetsWithPhotoUrls.get, tweetTotal.get)}%")
      domainsArr
    }

    val topDomains = domains.map((_, 1)).reduceByKey(_ + _)
      .map{case (topic, count) => (count, topic)}

    val topList = topDomains.take(10)
    println(s"Top 10 Domains (${rdd.count()} total):")
    topList.foreach{case (count, domain) => println(s"$domain ($count tweets)")}
  }

  def getPercentage(x: Long, y: Long) = (x * 100) / y

  def timeDiff(unit: TimeUnit, startTime: Long) =
    unit.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)
}
