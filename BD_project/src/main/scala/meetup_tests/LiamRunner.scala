package meetup_tests

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.{BufferedSource, Source}

object LiamRunner {

  def main(args: Array[String]):Unit = {
    val fromconsole = "sample_upcoming.txt"
    val fixedsample = "nice_upcoming.json"
    val fromconsoleurl = "sample_from_urlname.txt"
    val slightly_fixed_url_event = "nice_from_urlname.txt"
    val fixed_url_event = "nice_from_urlname.json"
    val spark = SparkSession.builder()
      .appName("Meetup Test")
      .master("local[4]")
      .getOrCreate()

    // response to searching for upcoming events into list of groups
    response_to_nice_json(fromconsole, fixedsample)
    val groupurl = group_url_from_upcoming(spark, fixedsample)
    for (value <- groupurl) {
      println(value)
    }

    // response to searching for all events by a group into tsv of relevant data
    response_to_nice_json(fromconsoleurl, slightly_fixed_url_event)
    indexed_json_to_json_array(slightly_fixed_url_event, fixed_url_event)
    val allevents_group = group_event_to_df(spark, fixed_url_event)
    allevents_group.show()

    saveDfToCsv(allevents_group, "to_keep.tsv")


  }
  def indexed_json_to_json_array(input: String, output: String): Unit = {
    val text = getTextContent(input).getOrElse("no events")
    val beginning = "\"0\": "
    val indexes = "\\n\"\\d+\": "
    val ending = "}\\n}"
    // replace with escaped quote if not preceded or followed by a new line or colon. or followed by a comma
//    val new_beg_text = text.replaceAll(beginning, "\"events\": [{\n")
    val new_beg_text = text.replaceAll(beginning, "\"events\": [")
    val new_mid_text = new_beg_text.replaceAll(indexes, "")
    val new_text = new_mid_text.replaceAll(ending, "}]\n}")
    val file = new File(output)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(new_text)
    bw.close()
  }

  def response_to_nice_json(input: String, output: String): Unit = {
    val text = getTextContent(input).getOrElse("no events")
    val remove_group = "([^:\\n][\\w= ])\\\"([^,:\\n])"
    // replace with escaped quote if not preceded or followed by a new line or colon. or followed by a comma
    val new_text = text.replaceAll(remove_group, "$1\\\\\"$2")

    val file = new File(output)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(new_text)
    bw.close()
  }

  def group_url_from_upcoming(spark: SparkSession, jsonpath: String): List[String] = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
      .select(explode($"events") as "event")
    val groupurl = origDF.select($"event.group.urlname").coalesce(1).collect().map(row => row(0).toString).toList
    groupurl
  }

  def group_event_to_df(spark: SparkSession, jsonpath: String): DataFrame = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
      .select(explode($"events") as "event")
    origDF.select($"event.id", $"event.name", $"event.is_online_event", $"event.status", $"event.yes_rsvp_count",
      $"event.rsvp_limit", $"event.waitlist_count", $"event.time", $"event.created", $"event.duration")
  }

  def saveDfToCsv(df: DataFrame, tsvOutput: String, sep: String = "\t"): Unit = {
    val tmpParquetDir = "Posts.tmp.parquet"

    df.repartition(1).write.
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
