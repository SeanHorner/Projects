package meetup_tests

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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

    // response to searching for upcoming events into list of groups
//    response_to_nice_json(fromconsole, fixedsample)
//    val groupurl = group_url_from_upcoming(spark, fixedsample)
//    for (value <- groupurl) {
//      println(value)
//    }

    // response to searching for all events by a group into tsv of relevant data
//    objects_to_array(fromconsoleurl, fixedsample)
    val someevents_group = group_event_to_df(spark, fixedsample)
    someevents_group.show()
    someevents_group.printSchema()
    saveDfToCsv(someevents_group, "data_50cities.tsv")

//    val allevents_group = several_group_event_to_df(spark, input_files_path)
//    allevents_group.show()
//    saveDfToCsv(someevents_group, "allevents.tsv")
  }
//  def indexed_json_to_json_array(input: String, output: String): Unit = {
//    val text = getTextContent(input).getOrElse("no events")
//    val beginning = "\"0\": "
//    val indexes = "\\n\"\\d+\": "
//    val ending = "}\\n}"
//    // replace with escaped quote if not preceded or followed by a new line or colon. or followed by a comma
////    val new_beg_text = text.replaceAll(beginning, "\"events\": [{\n")
//    val new_beg_text = text.replaceAll(beginning, "\"events\": [")
//    val new_mid_text = new_beg_text.replaceAll(indexes, "")
//    val new_text = new_mid_text.replaceAll(ending, "}]\n}")
//    val file = new File(output)
//    val bw = new BufferedWriter(new FileWriter(file))
//    bw.write(new_text)
//    bw.close()
//  }

//  def response_to_nice_json(input: String, output: String): Unit = {
//    val text = getTextContent(input).getOrElse("no events")
//    val remove_group = "([^:\\n][\\w= ])\\\"([^,:\\n])"
//    // replace with escaped quote if not preceded or followed by a new line or colon. or followed by a comma
//    val new_text = text.replaceAll(remove_group, "$1\\\\\"$2")
//
//    val file = new File(output)
//    val bw = new BufferedWriter(new FileWriter(file))
//    bw.write(new_text)
//    bw.close()
//  }

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
//    val myUDF = udf(description_scrubber.description_scrubber())
    val scrub = udf[String, String](description_scrubber.description_scrubber)
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
      .distinct()
    origDF.printSchema()
    origDF.select($"id", $"name", $"local_date", $"group.localized_location", $"is_online_event",
      //          $"description_occurred", $"meta_category.category_ids" ,
      $"duration", $"time", $"created", $"yes_rsvp_count", $"rsvp_limit", $"fee.accepts", $"fee.amount", $"description")
      .withColumn("scrubbed_description", scrub($"description"))
      .drop($"description")
  }

  def several_group_event_to_df(spark: SparkSession, jsonpaths: ListBuffer[String]): DataFrame = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"${jsonpaths(0)}")
      .select($"id", $"name", $"is_online_event", $"status", $"yes_rsvp_count",
        $"waitlist_count", $"time", $"created", $"duration")
    var toadd: DataFrame = null
    var result: DataFrame = null
    for (jsonpath <- jsonpaths) {
      toadd = spark.read.option("multiline", "true")
        .json(s"$jsonpath")

        .select($"id", $"name", $"local_date", $"group.localized_location", $"is_online_event",
//          $"description_occurred",
          $"category_ids", $"time", $"created", $"duration", $"yes_rsvp_count", $"rsvp_limit",
          $"fee.accepts", $"fee.amount")

      result = origDF.unionByName(toadd)
    }
    result.distinct()
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

}
