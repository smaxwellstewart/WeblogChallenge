/* Sessionize.scala */
package sessionize

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import java.io._

/*
 * My solution to step 1 of the Paytm Challenge.
 * I used a fixed window of 15 minutes and excluded non-GET requests as well as non-OK statuses.
 * I would have liked to complete much more but as my experience with both Scala and Spark is very
 * limited I spent a long time setting up environments.
 * I though you would prefer someone pushing themselves completely into the unknown rather than playing it safe (I have MapReduce experience but wanted to try Spark).
 * There are unit tests that can be run with Junit.
 * IMPROVEMENTS:
 *  - Sessionize by using a timeout period (wasn't sure if this was the actual due to wording)
 *  - Complete other steps
 * */


object Sessionize {
  def start = "2015-07-22T09:00:00.000Z" // start of data
  def window = 15 // fixed time window  from start, in minutes
  def dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def parseTime(line: String, format: SimpleDateFormat): Date = {
    val parts = line.split("\\s")
    // convert time to use milliseconds rather than microseconds
    val timeString = parts(0).substring(0, 20) + String.valueOf(Integer.parseInt(parts(0).substring(20, 26)) / 1000) + "Z"
    format.parse(timeString)

  }
  // we only want to look at 'hits': successful GET requests
  def validRequest(line: String): Boolean = {
    val parts = line.split("\\s")
    if (line.contains("GET") && parts(7) == "200") {
			return true
		}
		return false
  }
  // looking at window of data
  def inWindow(line: String, start: Date, end: Date): Boolean = {
    val t = parseTime(line, dateformat)
    if (t.after(start) && t.before(end)) {
			return true
		}
		return false
  }
  
  def main(args: Array[String]) {
    val logFile = "/Users/smaxwell-stewart/workspace/scala/paytm/sample.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val start = "2015-07-22T09:00:00.000Z"
    val end = "2015-07-22T09:15:00.000Z"
    
    
    // NOTE: wanted to refactor this to put in separate function but ran into scala type comilation errors
    val aggHits = logData
      // only want to look at valid requests
      .filter(line => validRequest(line))
      // only want to look at requests in window
      .filter(line => inWindow(line, dateformat.parse(start), dateformat.parse(end)))
      // map out key value pairs: key is IP and we count each hit once
      .map(line => (line.split("\\s")(2), 1))
      // aggregate all hits by IP
      .reduceByKey((a, b) => a + b)


      val out = new PrintWriter(new File("hits.csv"))
      aggHits.collect().foreach((line) => out.println("%s,%s".format(line._1, line._2)))
		  out.close()
  }

}
