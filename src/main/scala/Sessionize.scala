/* Sessionize.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Date
import java.text.SimpleDateFormat

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
  def inWindow(line: String, start: Date, end: Date, format: SimpleDateFormat): Boolean = {
    val t = parseTime(line, format)
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

    val aggHits = logData
      // only want to look at valid requests
      .filter(line => validRequest(line))
      // only want to look at requests in window
      .filter(line => inWindow(line, dateformat.parse(start), dateformat.parse(end), dateformat))
      // map out key value pairs: key is IP and we count each hit once
      .map(line => (line.split("\\s")(2), 1))
      // aggregate all hits by IP
      .reduceByKey((a, b) => a + b)

      // collect all data from distributed nodes and print out...
      aggHits.collect().foreach((line) => println("%s,%s".format(line(0), line(1))))

  }

}
