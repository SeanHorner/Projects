package meetuptrend

import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit ={

    // TODO: Might add a OAuth token for pulling Meetup API data directly in here.

    // Start our Spark SQL entry point
    val spark = SparkSession.builder()
      .appName("Tech Meetups Trend")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // Create a dummy folder to read from for schema creation.
    val staticDf = spark.read.option("header", "true").csv("dump")

    val df = spark.read.schema(staticDf.schema).option("header", "true").csv("dump").createOrReplaceTempView("view1")

    val q1 = spark.sql("SELECT view1.name " +
      "FROM view1 ").show()
//      "GROUP BY month, events_count " +
//      "ORDER BY events_count DESC").show()


  }
}
