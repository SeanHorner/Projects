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

    val millisToMin = udf[Int, Long](milToMin)
    val millisToQtr = udf[Int, Long](milToQtr)
    val millisToHour = udf[Int, Long](milToHr)
    val millisToDays = udf[Int, Long](milToDays)
    val tzAdj = udf[Long, String](timezoneAdj)


    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 1 analysis.
    // What is the trend of new tech vs established tech meetups? (e.g. number of meetups about C++ vs. Rust)

    val ada_count = df.filter('description.contains("ada ")).count()
    val android_count = df.filter('description.contains("android ")).count()
    val clojure_count = df.filter('description.contains("clojure ")).count()
    val cobol_count = df.filter('description.contains("cobol ")).count()
    val dart_count = df.filter('description.contains("dart ")).count()
    val delphi_count = df.filter('description.contains("delphi ")).count()
    val fortran_count = df.filter('description.contains("fortran ")).count()
    val ios_count = df.filter('description.contains("ios ")).count()
    val java_count = df.filter('description.contains("java ")).count()
    val javascript_count = df.filter('description.contains("javascript ")).count()
    val kotlin_count = df.filter('description.contains("kotlin ")).count()
    val labview_count = df.filter('description.contains("labview ")).count()
    val matlab_count = df.filter('description.contains("matlab ")).count()
    val pascal_count = df.filter('description.contains("pascal ")).count()
    val perl_count = df.filter('description.contains("perl ")).count()
    val php_count = df.filter('description.contains("php ")).count()
    val powershell_count = df.filter('description.contains("powershell ")).count()
    val python_count = df.filter('description.contains("python ")).count()
    val ruby_count = df.filter('description.contains("ruby ")).count()
    val rust_count = df.filter('description.contains("rust ")).count()
    val scala_count = df.filter('description.contains("scala ")).count()
    val sql_count = df.filter('description.contains("sql ")).count()
    val typescript_count = df.filter('description.contains("typescript ")).count()
    val vb_count = df.filter('description.contains("visual  basic")).count()
    val wolfram_count = df.filter('description.contains("wolfram ")).count()
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
      .orderBy('Year)

    Q1DF.show(10)

    Q1DF
      .select('Technology, 'Count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_1_data")

    val xAxis = Q1DF
      .select('Technology)
      .as[String]
      .collect()
      .toSeq

    val yAxis = Q1DF
      .select('Count)
      .as[Double]
      .collect()
      .toSeq



    outputConverter("question_1_data", "question_1/question_1.tsv")
    plotBar1(xAxis, yAxis, "Prevalence of Technologies", "question_1/q1", "Technology", "Occurrences")

    Q1DF.unpersist()

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 3 analysis.
    // What is the most popular time when events are created? (Find local_time/date)

    val Q3DF_byCount = df
      .filter('created.isNotNull && 'localized_location.isNotNull)
      .withColumn("timezoneAdjustment", tzAdj('localized_location))
      .withColumn("modulated_time_created",
        ('created+ 'timezoneAdjustment) % 86400000)
      .withColumn("hour", millisToHour('modulated_time_created))
      .groupBy($"hour")
      .count()
      .orderBy($"count".desc)
      .limit(5)

    Q3DF_byCount
      .select('hour, 'count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_3_data_byCount")

    outputConverter("question_3_data_byCount", "question_3/question_3_byCount.tsv")
    plotBar(Q3DF_byCount, "Top Event Creation Times", "question_3/q3_byCount", "Hour of Day", "Occurrences")

    Q3DF_byCount.unpersist()

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

    Q3DF
      .select('hour, 'count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_3_data")

    outputConverter("question_3_data", "question_3/question_3.tsv")
    plotBar(Q3DF, "Event Creation Times", "question_3/q3", "Hour of Day", "Occurrences")

    Q3DF.unpersist()

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 5 analysis.
    // Are events with longer durations more popular vs shorter ones?

    // Ranking by count, top 10 values
    val Q5DF_byCount = df
      .filter(df("duration").isNotNull)
      .withColumn("duration_minutes", millisToMin('duration))
      .groupBy('duration_minutes)
      .count()
      .orderBy($"count".desc)
      .limit(5)

    Q5DF_byCount
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data_byCount")

    outputConverter("question_5_data_byCount", "question_5/question_5_byCount.tsv")
    plotBar(Q5DF_byCount, "Top Event Duration", "question_5/q5_byCount")

    Q5DF_byCount.unpersist()

    // Values for up to 12 hours (halfday)
    val Q5DF_halfday = df
      .filter(df("duration").isNotNull)
      .withColumn("duration_min", millisToQtr('duration))
      .groupBy('duration_min)
      .count()
      .orderBy('duration_min)
      .limit(48)

    Q5DF_halfday
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data_halfday")

    outputConverter("question_5_data_halfday", "question_5/question_5_halfday.tsv")
    plotBar(Q5DF_halfday, "Event Duration (Half Day)", "question_5/q5_halfday", "Minutes", "Occurrences")

    Q5DF_halfday.unpersist()

    // Full set of values divided into days
    val Q5DF_fullset = df
      .filter(df("duration").isNotNull)
      .withColumn("duration_day", millisToDays('duration))
      .groupBy('duration_day)
      .count()
      .orderBy('duration_day)
      .filter('duration_day > 1)

    Q5DF_fullset
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data_fullset")

    outputConverter("question_5_data_fullset", "question_5/question_5_full.tsv")
    plotBar(Q5DF_fullset, "Event Duration (Full Set)", "question_5/q5_fullset", "Days", "Occurrences")

    Q5DF_fullset.unpersist()

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 13 analysis.
    // Has there been a change in planning times for events? (time - created)
    val Q13_byCount = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", millisToDays('time - 'created))
      .groupBy('prepping_period)
      .count()
      .orderBy('count.desc)
      .limit(5)

    Q13_byCount
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_13_data_byCount")

    outputConverter("question_13_data_byCount", "question_13/question_13_byCount.tsv")
    plotBar(Q13_byCount, "Top Planning Times", "question_13/q13_byCount")

    Q13_byCount.unpersist()

    val Q13DF = df
      .filter('created.isNotNull && 'time.isNotNull)
      .withColumn("prepping_period", millisToDays('time - 'created))
      .filter('prepping_period > 0)
      .groupBy('prepping_period)
      .count()
      .orderBy('prepping_period.asc)

    Q13DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_13_data")

    outputConverter("question_13_data", "question_13/question_13.tsv")
    plotBar(Q13DF, "Prevalence of Planning Times", "question_13/q13", "Days", "Occurrences")

    Q13DF.unpersist()
  }

  //-------------------------------------------------------------------------------------------------------
  //--------------------------------------METHODS----------------------------------------------------------
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

  def milToMin(millis: Long): Int = (millis/ 60000).toInt

  def milToQtr(millis: Long): Int = (millis / 900000).toInt*15

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

  def milToDays(millis: Long): Int = (millis / 86400000).toInt

  def timezoneAdj(str: String): Long = analysis_helper.citiesTimeAdj(str)

  def plotBar(df: DataFrame, title: String, fname: String, xLabel: String = "", yLabel:String = ""): Unit ={
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
      .xLabel(xLabel)
      .yLabel(yLabel)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(s"$fname.png"))
  }

  def plotBar1(x: Seq[String], y: Seq[Double], title: String, fname: String, xLabel: String = "", yLabel:String = ""): Unit ={
    val yAxis = y
    val xAxis = x

    // Plot a bar chart
    BarChart.custom(yAxis.map(Bar.apply))
      .title(title)
      .standard(xLabels = xAxis)
      .ybounds(lower = 0)
      .xLabel(xLabel)
      .yLabel(yLabel)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(s"$fname.png"))
  }
}
