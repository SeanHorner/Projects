package events_parsing

import org.apache.spark.sql.SparkSession

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
    val vb_count = df.filter('description.contains("visual basic")).count()
    val wolfram_count = df.filter('description.contains("wolfram")).count()
    val total_count = df.count()

    val Q1DF = Seq(
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
      ("Wolfram", 1988, wolfram_count),
      ("Total", 0, total_count)
    ).toDF("Technology", "Year", "Count")

    Q1DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_1_data")

    outputConverter("question_1_data", "output/question_1.tsv")

    // Beginning of Question 3 analysis.
    // What is the most popular time when events are created? (Find local_time/date)
    df
      .filter(df("created").isNotNull)
      .withColumn("modulated_time_created", ('created + ('time - 'local_time)) % 86400000)
      .withColumn("hour", 'modulated_time_created.divide(3600000))
      .groupBy($"modulated_time_created")
      .count()
      .orderBy($"count".desc)
      .show(10)
    // output from sample: 0.289444 hours (12:17:22 AM) = 202 occurrences

    // Full output for graphing
    val Q3DF = df
      .filter(df("created").isNotNull)
      .withColumn("modulated_time_created", ('created + ('time - 'local_time)) % 86400000)
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

    outputConverter("question_3_data", "output/question_3.tsv")

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

    outputConverter("question_5_data", "output/question_5.tsv")

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

    outputConverter("question_13_data", "output/question_13.tsv")

  }

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
}
