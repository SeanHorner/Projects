package events_parsing

import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

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

    val df = spark.read.parquet("all_cities_data.parquet")

    df.show(10)

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 1 analysis.
    // What is the trend of new tech vs established tech meetups? (e.g. number of meetups about C++ vs. Rust)

    val ada_count = df.filter('description.contains("ada")).count()
    val android_count = df.filter('description.contains("android")).count()
    val clojure_count = df.filter('description.contains("clojure")).count()
    val cobol_count = df.filter('description.contains("cobol")).count()
    val dart_count = df.filter('description.contains("dart")).count()
    val delphi_count = df.filter('description.contains("delphi")).count()
    val fortran_count = df.filter('description.contains("fortran")).count()
    val ios_count = df.filter('description.contains("ios")).count()
    val java_count = df.filter('description.contains("java")).count()
    val javascript_count = df.filter('description.contains("javascript")).count()
    val kotlin_count = df.filter('description.contains("kotlin")).count()
    val labview_count = df.filter('description.contains("labview")).count()
    val matlab_count = df.filter('description.contains("matlab")).count()
    val pascal_count = df.filter('description.contains("pascal")).count()
    val perl_count = df.filter('description.contains("perl")).count()
    val php_count = df.filter('description.contains("php")).count()
    val powershell_count = df.filter('description.contains("powershell")).count()
    val python_count = df.filter('description.contains("python")).count()
    val ruby_count = df.filter('description.contains("ruby")).count()
    val rust_count = df.filter('description.contains("rust")).count()
    val scala_count = df.filter('description.contains("scala")).count()
    val sql_count = df.filter('description.contains("sql")).count()
    val typescript_count = df.filter('description.contains("typescript")).count()
    val vb_count = df.filter('description.contains("visual  basic")).count()
    val wolfram_count = df.filter('description.contains("wolfram")).count()
    val total_count = df.count()

    val Q1DF =
      Seq(
      ("Ada", 1983, ada_count),
      ("Android", 2008, android_count),
      ("Clojure", 2007, clojure_count),
      ("COBOL", 1959, cobol_count),
      ("Dart", 2011, dart_count),
      ("Delphi", 1995, delphi_count),
      ("FORTRAN", 1957, fortran_count),
      ("iOS", 2007, ios_count),
      ("Java", 1995, java_count),
      ("JavaScript", 1995, javascript_count),
      ("Kotlin", 2011, kotlin_count),
      ("LabVIEW", 1986, labview_count),
      ("MATLAB", 1984, matlab_count),
      ("Pascal", 1970, pascal_count),
      ("Perl", 1987, perl_count),
      ("PHP", 1995, php_count),
      ("PowerShell", 2006, powershell_count),
      ("Python", 1990, python_count),
      ("Ruby", 1995, ruby_count),
      ("Rust", 2010, rust_count),
      ("Scala", 2003, scala_count),
      ("SQL", 1978, sql_count),
      ("TypeScript", 2012, typescript_count),
      ("Visual Basic", 1991, vb_count),
      ("Wolfram", 1988, wolfram_count)
    ).toDF("Technology", "Year", "Count")
      .orderBy('year)

    Q1DF.show(10)

    Q1DF
      .select('Technology, 'Count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_1_data")

    outputConverter("question_1_data", "output/question_1.tsv")

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 3 analysis.
    // What is the most popular time when events are created? (Find local_time/date)
    val millisToHour = udf[Int, Long](milToHr)
    val tzAdj = udf[Long, String](timezoneAdj)

    df
      .filter('created.isNotNull && 'localized_location.isNotNull)
      .withColumn("timezoneAdjustment", tzAdj('localized_location))
      .withColumn("modulated_time_created",
        ('created+ 'timezoneAdjustment) % 86400000)
      .withColumn("hour", millisToHour('modulated_time_created))
      .groupBy($"hour")
      .count()
      .orderBy($"count".desc)
      .show(10)
    // output from sample: 0.289444 hours (12:17:22 AM) = 202 occurrences

    // Full output for graphing
    val Q3DF = df
      .filter('created.isNotNull && 'localized_location.isNotNull)
      .withColumn("timezoneAdjustment", tzAdj('localized_location))
      .withColumn("modulated_time_created",
        ('created+ 'timezoneAdjustment) % 86400000)
      .withColumn("hour", millisToHour('modulated_time_created))
      .groupBy($"hour")
      .count()
      .orderBy($"hour")

    Q3DF.show(10)

    Q3DF
      .select('hour, 'count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_3_data")

//    plotBar(Q3DF, "Event Creation Times", "q3")

    Q3DF.unpersist()

    outputConverter("question_3_data", "output/question_3.tsv")

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 5 analysis.
    // Are events with longer durations more popular vs shorter ones?
    val millisToQtr = udf[Int, Long](milToQtr)
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
      .withColumn("duration_min", millisToQtr('duration))
      .groupBy('duration_min)
      .count()
      .orderBy('duration_min)

    Q5DF.show(10)

    Q5DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data")

//    plotBar(Q5DF, "Event Duration", "q5")

    Q5DF.unpersist()

    outputConverter("question_5_data", "output/question_5.tsv")

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 13 analysis.
    // Has there been a change in planning times for events? (time - created)
    val Q13_most = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", millisToQtr('created - 'time))
      .groupBy('prepping_period)
      .count()
      .orderBy('count.desc)

    Q13_most.show(10)

    val Q13DF = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", millisToQtr('time - 'created))
      .filter('prepping_period > 0)
      .groupBy('prepping_period)
      .count()
      .orderBy('prepping_period.asc)

    Q13DF.show(10)

    Q13DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_13_data")

//    plotBar(Q13DF, "Prevalence of Planning Times", "q13")

    Q13DF.unpersist()

    outputConverter("question_13_data", "output/question_13.tsv")
  }

  //-------------------------------------------------------------------------------------------------------

  def outputConverter(searchDir: String, tsvOutput: String): Unit = {
    val dir = new File(searchDir)
    val newFileRgex = ".*part-00000.*.csv"
    val tmpTsvFile = dir
      .listFiles
      .filter(_.toString.matches(newFileRgex))(0)
      .toString
    new File(tmpTsvFile).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }

  def milToHr(millis: Long): Int = {
    millis match {
      case m if 0 <= m && m < 3600000 => 0
      case m if 3600000 <= m && m < 7200000 => 1
      case m if 7200000 <= m && m < 10800000 => 2
      case m if 10800000 <= m && m < 14400000 => 3
      case m if 14400000 <= m && m < 18000000 => 4
      case m if 18000000 <= m && m < 21600000 => 5
      case m if 21600000 <= m && m < 25200000 => 6
      case m if 25200000 <= m && m < 28800000 => 7
      case m if 28800000 <= m && m < 32400000 => 8
      case m if 32400000 <= m && m < 36000000 => 9
      case m if 36000000 <= m && m < 39600000 => 10
      case m if 39600000 <= m && m < 43200000 => 11
      case m if 43200000 <= m && m < 46800000 => 12
      case m if 46800000 <= m && m < 50400000 => 13
      case m if 50400000 <= m && m < 54000000 => 14
      case m if 54000000 <= m && m < 57600000 => 15
      case m if 57600000 <= m && m < 61200000 => 16
      case m if 61200000 <= m && m < 64800000 => 17
      case m if 64800000 <= m && m < 68400000 => 18
      case m if 68400000 <= m && m < 72000000 => 19
      case m if 72000000 <= m && m < 75600000 => 20
      case m if 75600000 <= m && m < 79200000 => 21
      case m if 79200000 <= m && m < 82800000 => 22
      case m if 82800000 <= m && m < 86400000 => 23
    }
  }

  def timezoneAdj(str: String): Long = analysis_helper.citiesTimeAdj(str)

  def milToQtr(millis: Long): Int = (millis / 900000).toInt*15

  def plotBar(df: DataFrame, title: String, fname: String): Unit ={
    // Convert dataframe into sequence of doubles for axes
    val seq =
      df.collect()
      .map(row => row.toSeq
        .map(_.toString.toDouble))
      .toSeq
      .flatten

    var yAxis = Seq[Double]()
    var xAxis = Seq[String]()

    // Populate x-axis and y-axis
    for(i <- seq.indices){
      if(i%2 == 0){
        xAxis = xAxis :+ seq(i).toInt.toString
      }
    }
    for(i <- seq.indices){
      if(i%2 != 0){
        yAxis = yAxis :+ seq(i)
      }
    }
    // Plot a bar chart
    BarChart.custom(yAxis.map(Bar.apply))
      .title(title)
      .standard(xLabels = xAxis)
      .ybounds(lower = 0)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(s"graphs/$fname.png"))
  }

  def stackedBarPlot(df: DataFrame, title: String, fname: String): Unit = {
    val data = Seq[Seq[Double]](
      Seq(1, 2, 3),
      Seq(4, 5, 6),
      Seq(3, 4, 1),
      Seq(2, 3, 4)
    )
    BarChart
      .stacked(
        data,
        labels = Seq("one", "two", "three")
      )
      .title("Stacked Bar Chart Demo")
      .xAxis(Seq("a", "b", "c", "d"))
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"graphs/$fname.png"))
  }
}
