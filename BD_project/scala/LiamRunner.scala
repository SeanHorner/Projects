package meetup_tests

import java.io.{BufferedWriter, File, FileWriter}

import meetup_tests.Analysis.date_to_month
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}

object LiamRunner {

  def main(args: Array[String]):Unit = {
    val fixedsample = "data_50cities_fixed.json"
    val input_files_titles = List("GoBridge", "JuniorDeveloperHappyHour", "LearnWordPressDiscussions", "UXWizardsBayArea", "WomenWhoCodeSF")
    val input_files_path: ListBuffer[String] = ListBuffer()
    for (file_name <- input_files_titles){
      input_files_path.append("sample_data/" + file_name + ".json")
    }
    val fromconsoleurl = "data_50cities.json"
    val spark = SparkSession.builder()
      .appName("Meetup Test")
      .master("local[4]")
      .getOrCreate()

//    val someevents_group = group_event_to_df(spark, fixedsample)
//    saveDfToCsv(someevents_group, "data_50cities_v3.tsv")

    val df = spark.read
      .option("header", true)
      .option("delimiter", "\t")
      .csv("data_50cities_v3.tsv")
    val analysis14 = Analysis.online_event_count_trend(spark, df)
    saveDfToCsv(analysis14, "q14_results.tsv")
    val analysis11 = Analysis.fee(spark, df)
    saveDfToCsv(analysis11, "q11_results.tsv")
  }

  def group_url_from_upcoming(spark: SparkSession, jsonpath: String): List[String] = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
      .select(explode($"events") as "event")
    val groupurl = origDF.select($"event.group.urlname").coalesce(1).collect().map(row => row(0).toString).toList
    groupurl
  }

  def group_url_from_groups(spark: SparkSession, jsonpath: String): List[String] = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
    val groupurl = origDF.select($"urlname").coalesce(1).collect().map(row => row(0).toString).toList
    groupurl
  }

  def objects_to_array(input: String, output: String): Unit = {
    var openedFile: BufferedSource = Source.fromFile(input)
    val file = new File(output)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("[")
    for (line <- Source.fromFile(input).getLines) {
      bw.append(line)
      bw.append(", ")
    }
    bw.append("{}]")
    bw.close()
  }

  def group_event_to_df(spark: SparkSession, jsonpath: String): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions.udf

    val scrub = udf[String, String](description_scrubber.description_scrubber)
    val month = udf[String, String](date_to_month)

    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
      .distinct()
    origDF.select($"id", $"name", $"group.name" as "group_name", $"group.urlname", $"venue.id" as "v_id",
      $"venue.name" as "v_name", $"local_date", month($"local_date".cast(StringType)) as "date_month", $"local_time",
      $"group.localized_location", $"is_online_event", $"status",
//      $"meta_category.category_ids" ,
      $"duration", $"time", $"created", $"yes_rsvp_count", $"rsvp_limit", $"fee.accepts", $"fee.amount",
      scrub($"description") as "description")
  }

  def saveDfToCsv(df: DataFrame, tsvOutput: String, sep: String = "\t"): Unit = {
    val tmpParquetDir = "Posts.tmp.parquet"

    df.coalesce(1).write.
          format("com.databricks.spark.csv").
          option("header", true).
          option("delimiter", sep).
          save(tmpParquetDir)

    val dir = new File(tmpParquetDir)
    val newFileRgex = ".*part-00000.*.csv"
    val tmpTsvFile = dir
      .listFiles
      .filter(_.toString.matches(newFileRgex))(0)
      .toString
    (new File(tmpTsvFile)).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }

  def getTextContent(filename: String): Option[String] = {
    var openedFile : BufferedSource = null
    try{
      openedFile = Source.fromFile(filename)
      Some(openedFile.getLines().mkString("\n"))
    } finally{
      if (openedFile != null) openedFile.close()
    }
  }

  def date_to_month(date: String): String = {
    var short_date = date + " "
    if (short_date.matches("\\d+-\\d+-\\d+ ")) {
      short_date = date.replaceFirst("(\\d+-\\d+)-\\d+", "$1")
    }
    short_date
  }
}
