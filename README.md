# banno_twitter_realtime

# Requirements
  * Apache Spark 2.2.0
  * Scala
  * Twitter App (Consumer Key, Consumer Secret, Access Token, Access Token Secret)
 
# Example
  * Building
    - `sbt assembly`
  * Running
    - `[SPARK_DIRECTORY]/bin/spark-submit --class "TwitterStreamer" target/scala-2.11/banno_twitter_realtime-assembly-1.0.jar [CONSUMER_KEY] [CONSUMER_SECRET] [ACCESS_TOKEN] [ACCESS_TOKEN_SECRET]`
