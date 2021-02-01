/*
Quan Vu, Staging Project - Meetup Tech Events Analysis
Questions 6 and 12
 */

package meetuptrend

import com.cibo.evilplot.colors.HSL
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

object Runner {
  def main(args: Array[String]): Unit ={

    // Solved java.io exception error of missing winutils.exe
    //System.setProperty("hadoop.home.dir", "C:/Hadoop")

    // Start our Spark SQL entry point
    val spark = SparkSession.builder()
      .appName("Tech Meetups Trend")
      .master("local[4]")
      .getOrCreate()

    // Set console log to give warnings or worse
    spark.sparkContext.setLogLevel("WARN")

    // Create view of dataframe for Spark SQL
    val df = spark.read.option("header", "true")
      .option("sep", "\t")
      .csv("data_50cities_v3.tsv")
      .createOrReplaceTempView("view1")

    // Find tech event counts for each year starting at 2002 to 2020
    val q1 = spark.sql("SELECT SUBSTRING(view1.local_date, 1, 4) AS year, COUNT(*) AS counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2%' " +
      "AND view1.local_date <= '2021' " +
      "GROUP BY year " +
      "ORDER BY year ASC")//.show()

    // Store results into csv files
    //q1.coalesce(1).write.format("com.databricks.spark.csv").save("results/yearly_events")

    // Question 6: Find the tech event counts for each month in 2020
    val q2 = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, COUNT(*) AS counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2020%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()

    //q2.coalesce(1).write.format("com.databricks.spark.csv").save("results/monthly_events_2020")

    // Question 12: How has the capacity (rsvp_limit) changed over time? Find rsvp_avg.
    // TODO: 2006 has a spike, but why?
    val q3 = spark.sql("SELECT SUBSTRING(view1.local_date, 1, 4) AS year, " +
      "ROUND(AVG(view1.rsvp_limit), 2) AS rsvp_avg " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2%' AND view1.local_date BETWEEN '2005' AND '2021' " +
      "GROUP BY year " +
      "ORDER BY year")//.show()

    //q3.coalesce(1).write.format("com.databricks.spark.csv").save("results/yearly_rsvp")

    // Sub-question: Find avg rsvp_limit for each month in 2020.
    val q4 = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, " +
      "ROUND(AVG(view1.rsvp_limit), 2) AS rsvp_avg " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2020%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()

    //q4.coalesce(1).write.format("com.databricks.spark.csv").save("results/monthly_rsvp_2020")

    // Plot bar charts
    plot(q1, "Tech Events From 2002-2020", "plot1")
    plot(q2, "Tech Events in 2020", "plot2")
    plot(q3, "RSVP Limit Over Years (2005-2020)", "plot3")
    plot(q4, "RSVP Limit For Each Month in 2020", "plot4")
  }

  // Plot bar charts using Dataframe results
  def plot(df: DataFrame, title: String, fname: String): Unit ={
    // Convert dataframe into sequence of doubles for axes
    val seq = df.collect().map(row => row.toSeq.map(_.toString.toDouble)).toSeq.flatten

    var counts = Seq[Double]()  // y-axis
    var labels = Seq[String]()  // x-axis

    // Populate x-axis and y-axis
    for(i <- 0 until seq.length){
      if(i%2 == 0){
        labels = labels :+ (seq(i).toInt.toString)
      }
    }
    for(i <- 0 until seq.length){
      if(i%2 != 0){
        counts = counts :+ seq(i)
      }
    }
    // Plot a bar chart
    BarChart.custom(counts.map(Bar.apply))
      .title(title)
      .standard(xLabels = labels)
      .ybounds(lower = 0)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(s"C:/Users/quanv/IdeaProjects/stagingpj-meetup/${fname}.png"))
  }
}
