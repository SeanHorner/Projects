package events_parsing

import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

object spark_runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Meetup Trends")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    meetup_analysis(spark)

  }

  def meetup_analysis(spark: SparkSession): Unit = {
    import spark.implicits._

    val df =
      spark.read
        .option("delimiter", "\t")
        .option("header", true)
        .option("inferSchema", "true")
        .csv("data_50cities_v2.tsv")

    df.filter($"scrubbed_description".contains("python")).show(5)


    // Beginning of Question 1 analysis.
    // What is the trend of new tech vs established tech meetups? (e.g. number of meetups about C++ vs. Rust)



    // Beginning of Question 3 analysis.
    // What is the most popular time when events are created? (Find local_time/date)
    df
      .filter(df("created").isNotNull)
      .withColumn("modulated_time_created", $"created" % 86400000)
      .withColumn("hour", 'modulated_time_created.divide(3600000))
      .groupBy($"modulated_time_created")
      .count()
      .orderBy($"count".desc)
      .show(10)
    // output from sample: 0.289444 hours (12:17:22 AM) = 202 occurrences

    // Full output for graphing
    val Q3DF = df
      .filter(df("created").isNotNull)
      .withColumn("modulated_time_created", $"created" % 86400000)
      .withColumn("hour", 'modulated_time_created.divide(3600000))
      .groupBy($"modulated_time_created")
      .count()
      .orderBy($"modulated_time_created")

    Q3DF.show()

    Q3DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_3_data")

    Q3DF.unpersist()

    Files.move(
      Paths.get("question_3_data/*.csv"),
      Paths.get("output/question_3.tsv")
    )

    // Beginning of Question 5 analysis.
    // Are events with longer durations more popular vs shorter ones?
    df
      .filter(df("duration").isNotNull)
      .groupBy($"duration")
      .count()
      .orderBy($"count".desc)
      .show(10)
    // output from sample: 2 hours = 70484 occurrences

    // Full time-scale numbers for graphing
    val Q5DF = df
      .filter(df("duration").isNotNull)
      .groupBy($"duration")
      .count()
      .orderBy($"duration")

    Q5DF.show()

    Q5DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data")

    Q5DF.unpersist()

    // Beginning of Question 13 analysis.
    // Has there been a change in planning times for events? (time - created)
    df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", 'created - 'time)
      .groupBy('prepping_period)
      .count()
      .orderBy('count.desc)
      .show(10)

    val Q13DF = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", 'time - 'created)
      .groupBy('prepping_period)
      .count()
      .orderBy('prepping_period.desc)

    Q13DF.show()

    Q13DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_13_data")

    Q13DF.unpersist()

  }

  def csvCombiner(i: Int): Unit = {

  }

  def initialHeader(i: Int): String = {
    var result = ""

    i match {
      case 1 => result = ""
      case 2 => result = ""
      case 3 => result = "Mod_Time_Created\tCount\n"
      case 4 => result = ""
      case 5 => result = "Duration\tCount\n"
      case 6 => result = ""
      case 7 => result = ""
      case 8 => result = ""
      case 9 => result = ""
      case 10 => result = ""
      case 11 => result = ""
      case 12 => result = ""
      case 13 => result = "Prepping_Period\tCount\n"
      case 14 => result = ""
      case 15 => result = ""
    }

    result
  }

}
