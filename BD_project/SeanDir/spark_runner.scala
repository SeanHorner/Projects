package events_parsing

import org.apache.spark.sql.SparkSession

import java.io.{File, BufferedWriter}
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

//    val df =
//      spark.read
//        .option("delimiter", "\t")
//        .option("header", true)
//        .option("inferSchema", "true")
//        .csv("data_50cities_v2.tsv")
//
//    df.filter($"scrubbed_description".contains("python")).show(5)

    val df = spark.read.parquet("all_cities_data.parquet")


    // Beginning of Question 1 analysis.
    // What is the trend of new tech vs established tech meetups? (e.g. number of meetups about C++ vs. Rust)

    val total_count = df.count()
    val python_count = df.filter('scrubbed_description.contains("python")).count()
    val scala_count = df.filter('scrubbed_description.contains("scala")).count()
    val powershell_count = df.filter('scrubbed_description.contains("powershell")).count()
    val rust_count = df.filter('scrubbed_description.contains("rust")).count()
    val dart_count = df.filter('scrubbed_description.contains("dart")).count()
    val kotlin_count = df.filter('scrubbed_description.contains("kotlin")).count()
    val typescript_count = df.filter('scrubbed_description.contains("typescript")).count()
    val clojure_count = df.filter('scrubbed_description.contains("clojure")).count()
    val android_count = df.filter('scrubbed_description.contains("android")).count()
    val ios_count = df.filter('scrubbed_description.contains("ios")).count()
    val matlab_count = df.filter('scrubbed_description.contains("matlab")).count()
    val wolfram_count = df.filter('scrubbed_description.contains("wolfram")).count()
    val labview_count = df.filter('scrubbed_description.contains("labview")).count()
    val ada_count = df.filter('scrubbed_description.contains("ada")).count()
    val perl_count = df.filter('scrubbed_description.contains("perl")).count()
    val php_count = df.filter('scrubbed_description.contains("php")).count()
    val ruby_count = df.filter('scrubbed_description.contains("ruby")).count()
    val java_count = df.filter('scrubbed_description.contains("java")).count()
    val javascript_count = df.filter('scrubbed_description.contains("javascript")).count()
    val vb_count = df.filter('scrubbed_description.contains("visual basic")).count()
    val delphi_count = df.filter('scrubbed_description.contains("delphi")).count()
    val sql_count = df.filter('scrubbed_description.contains("sql")).count()
    val pascal_count = df.filter('scrubbed_description.contains("pascal")).count()
    val cobol_count = df.filter('scrubbed_description.contains("cobol")).count()
    val fortran_count = df.filter('scrubbed_description.contains("fortran")).count()
    val _count = df.filter('scrubbed_description.contains("")).count()


    // Beginning of Question 3 analysis.
    // What is the most popular time when events are created? (Find local_time/date)
    df
      .filter(df("created").isNotNull)
      .withColumn("modulated_time_created", ('created - ('time - 'local_time)) % 86400000)
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
      .save("output/question_3_data")

    Q3DF.unpersist()

    toOutput("question_3_data", "output/question_3.tsv")

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

    toOutput("question_5_data", "question_5.tsv")

    // Beginning of Question 13 analysis.
    // Has there been a change in planning times for events? (time - created)
    val Q13_most = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", 'created - 'time)
      .groupBy('prepping_period)
      .count()
      .orderBy('count.desc)

    Q13_most.show(10)

    val Q13DF = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", 'time - 'created)
      .groupBy('prepping_period)
      .count()
      .orderBy('prepping_period.asc)

    Q13DF.show()

    Q13DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_13_data")

    Q13DF.unpersist()

    toOutput("question_13_data", "output/question_13.tsv")

  }

//  def csvCombiner(i: Int): Unit = {
//
//  }
//
//  def initialHeader(i: Int): String = {
//    var result = ""
//
//    i match {
//      case 1 => result = ""
//      case 2 => result = ""
//      case 3 => result = "Mod_Time_Created\tCount\n"
//      case 4 => result = ""
//      case 5 => result = "Duration\tCount\n"
//      case 6 => result = ""
//      case 7 => result = ""
//      case 8 => result = ""
//      case 9 => result = ""
//      case 10 => result = ""
//      case 11 => result = ""
//      case 12 => result = ""
//      case 13 => result = "Prepping_Period\tCount\n"
//      case 14 => result = ""
//      case 15 => result = ""
//    }
//
//    result
//  }

  def toOutput(searchDir: String, tsvOutput: String, sep: String = "\t"): Unit = {
    val dir = new File(searchDir)
    val newFileRgex = ".*part-00000.*.csv"
    val tmpTsvFile = dir
      .listFiles
      .filter(_.toString.matches(newFileRgex))(0)
      .toString
    (new File(tmpTsvFile)).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }

  val language_list = List(
    "bcpl",
    "logo",
    "b",
    "pascal",
    "forth",
    "c",
    "smalltalk",
    "prolog",
    "scheme",
    "sql",
    "c++",
    "ada",
    "common lisp",
    "matlab",
    "eiffel",
    "objective-c",
    "labview",
    "erlang",
    "perl",
    "tcl",
    "wolfram",
    "haskell",
    "python",
    "visual basic",
    "lua",
    "r",
    "ruby",
    "java",
    "delphi",
    "javascript",
    "php",
    "rebol",
    "c#",
    "d",
    "scratch",
    "groovy",
    "scala",
    "f#",
    "powershell",
    "clojure",
    "nim",
    "node.js",
    "go",
    "rust",
    "dart",
    "kotlin",
    "elixir",
    "julia",
    "typescript",
    "swift",
    "crystal",
    "elm",
    "hack",
    "haxe",
    "zig",
    "reason",
    "ballerina"
  )

  val notable_languages = Map[Int, String](
    2000 -> "actionscript",
    2001 -> "c#",
    2002 -> "d",
    2003 -> "groovy",
    2004 -> "scala",
    2005 -> "f#",
    2006 -> "powershell",
    2007 -> "clojure",
    2008 -> "nim",
    2009 -> "node.js",
    2010 -> "rust",
    2011 -> "dart"
  )

}
