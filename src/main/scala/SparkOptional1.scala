import scala.util.matching.Regex
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object SparkOptional1 {

    def getSparkSession: SparkSession = {
        val spark = SparkSession
                .builder()
                .appName("Spark Optional task 1")
                .master("local[*]")
                .getOrCreate()
        spark
    }

    def main(args: Array[String]): Unit = {
        val spark = getSparkSession

        val sc = spark.sparkContext

        val log_file = "/var/folders/1d/_0pnt2994vg5xvvvj4fk41qr0000gp/T/sparkfinalassignment/src/resources/access_log.txt"

        val log_rdd = sc.textFile(log_file)

        val logPattern: Regex = """(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\d+|-)""".r

        def parseLogLine(logline: String): Option[(String, String, Int, Int)] = {
            logPattern.findFirstMatchIn(logline) match {
                case Some(m) => Some((m.group(1), m.group(5).split(" ")(1), m.group(6).toInt, if (m.group(7) != "-") m.group(7).toInt else 0))
                case None => None
            }
        }

        val parsedRdd = log_rdd.flatMap(parseLogLine)

        val uniqueIps = parsedRdd.map(x => x._1).distinct().collect()

        val ipsWith200 = parsedRdd.filter(x => x._3 == 200).map(x => (x._1, 1)).reduceByKey(_+_).collect()

        val numOf400s = parsedRdd.filter(x => x._3 >= 400 && x._3 < 500).count()

        val reqsWith5000 = parsedRdd.filter(x => x._4 > 5000).count()

        val requestWithMoreHits = parsedRdd.map(x => (x._2, 1)).reduceByKey(_+_).takeOrdered(1)(Ordering[Int].reverse.on(_._2))

        val requestWithMore404 = parsedRdd.filter(x => x._3 == 404).map(x => (x._2, 1)).reduceByKey(_+_).takeOrdered(1)(Ordering[Int].reverse.on(_._2))

        println("Number of unique ips:")

        uniqueIps.foreach(println)

        println("Number of requests with 200 response")

        ipsWith200.foreach(println)

        println(s"Number of ips with 4xx status: $numOf400s")

        println(s"Number of requests with more than 5000 bytes as response: $reqsWith5000")

        print(s"Request with more hits: ")

        requestWithMoreHits.foreach(println)

        print(s"Request with more 404 response: ")

        requestWithMore404.foreach(println)

        Thread.sleep(Long.MaxValue)

        spark.stop() 
    }
}

