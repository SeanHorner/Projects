
/*
Quan Vu, Staging Project - Meetup Tech Events Analysis
Questions 6 and 12
 */
package meetuptrend

import org.apache.spark.sql.{DataFrame, SparkSession}

object Analysis {
  def main(args: Array[String]): Unit = {

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
      "ORDER BY year ASC") //.show()

    // Store results into csv files
    q1.coalesce(1).write.format("com.databricks.spark.csv")
      .save("results/yearly_events")

    // Question 6: Find the tech event counts for each month in 2020
    val q2 = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, COUNT(*) AS counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2020%' " +
      "GROUP BY months " +
      "ORDER BY months") //.show()

    q2.coalesce(1).write.format("com.databricks.spark.csv")
      .save("results/monthly_events_2020")

    // Question 12: How has the capacity (rsvp_limit) changed over time? Find rsvp_avg.
    // TODO: 2006 has a spike, but why?
    val q3 = spark.sql("SELECT SUBSTRING(view1.local_date, 1, 4) AS year, " +
      "ROUND(AVG(view1.rsvp_limit), 2) AS rsvp_avg " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2%' AND view1.local_date BETWEEN '2005' AND '2020' " +
      "GROUP BY year " +
      "ORDER BY year") //.show()

    q3.coalesce(1).write.format("com.databricks.spark.csv")
      .save("results/yearly_rsvp")

    // Sub-question: Find avg rsvp_limit for each month in 2020.
    val q4 = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 1) AS months, " +
      "ROUND(AVG(view1.rsvp_limit), 2) AS rsvp_avg " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2020%' " +
      "GROUP BY months " +
      "ORDER BY months") //.show()

    q4.coalesce(1).write.format("com.databricks.spark.csv")
      .save("results/monthly_rsvp_2020")

    // Plot bar charts
    //plot(q1, "Tech Events From 2002-2020", "plot1")
    //plot(q2, "Tech Events in 2020", "plot2")
    //plot(q3, "RSVP Limit Over Years (2005-2020)", "plot3")
    //plot(q4, "RSVP Limit For Each Month in 2020", "plot4")
  }
}