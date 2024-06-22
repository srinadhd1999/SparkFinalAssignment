import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import javax.xml.crypto.Data

object SparkOptional2 {
    def getSparkSession: SparkSession = {
        val spark = SparkSession
                .builder()
                .appName("Spark Optional task 1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.port", "7077")
                .master("local[*]")
                .getOrCreate()
        spark
    }

    def readCsv(path: String, spark: SparkSession): DataFrame = {
        val df = spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
        df
    }

    def printDataFrame(df: DataFrame): Unit = {
        df.show
    }

    def writeDataFrameToDb(df: DataFrame, name: String): Unit = {
        df.write.format("jdbc")
        .option("url",sys.env.getOrElse("JDBC_URL", "")+"final_task")
        .option("dbtable", name)
        .option("user", sys.env.getOrElse("DB_USER", ""))
        .option("password", sys.env.getOrElse("DB_PASS", ""))
        .mode(SaveMode.Overwrite)
        .save()
    }

    def main(args: Array[String]) = {
        val spark = getSparkSession

        //books sales df
        val booksSalesDf = readCsv("/var/folders/1d/_0pnt2994vg5xvvvj4fk41qr0000gp/T/sparkfinalassignment/src/resources/booksStore.csv", spark)

        //books df
        val booksDf = booksSalesDf.select("book_id", "title", "author_id", "author_name", "genre", "publish_date").distinct()

        //authors df
        val authorsDf = booksSalesDf.select("author_id", "author_name").distinct()

        //sales df
        val salesDf = booksSalesDf.select("sale_id", "book_id", "sale_date", "quantity", "price")

        //sales by titles df
        val salesByTitlesDf = booksSalesDf.groupBy("title").agg(sum("quantity").alias("total_sales"))

        //sales by month df
        val salesByMonthColDf = booksSalesDf.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd")).withColumn("sale_month", month(col("sale_date")))
        val salesByMonthDf = salesByMonthColDf.groupBy("sale_month").agg(sum("quantity").alias("total_sales"))

        //sales by author df
        val salesByAuthorDf = booksSalesDf.groupBy("author_name").agg(sum("quantity").alias("total_sales"))

        //books sales df
        writeDataFrameToDb(booksSalesDf, "book_sales")
        //books df
        writeDataFrameToDb(booksDf, "books")
        //authors df
        writeDataFrameToDb(authorsDf, "authors")
        //sales df
        writeDataFrameToDb(salesDf, "sales")
        //sales by titles df
        writeDataFrameToDb(salesByTitlesDf, "sales_by_titles")
        //sales by month df
        writeDataFrameToDb(salesByMonthDf, "sales_by_months")
        //sales by authors df
        writeDataFrameToDb(salesByAuthorDf, "sales_by_authors")

        spark.stop()
    }
}