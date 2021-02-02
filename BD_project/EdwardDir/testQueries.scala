package review

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{asc, count, desc, explode, max, round, second}

object testQueries {
  def main(args: Array[String]): Unit = {
    val appName = "reader"
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    //This dataframe is created using the tsv file
    //val events = spark.read.option("sep", "\t").option("header", "true").csv(args(0))
    //  .withColumn("rsvp_count", $"yes_rsvp_count".cast("Int"))

    val events = spark.read.parquet(args(0))
      .withColumn("rsvp_count", $"yes_rsvp_count".cast("Int"))

    //This dataframe just has the city (localized_location) and a column containing how many events were
    //held in the city.
    val cities = events.select($"localized_location".as("city"))
      .groupBy("city")
      .agg(count("city").as("number of events"))
      .sort(functions.desc("number of events"))

   // cities.show(false)

    //Writes the answer to question 8 to file.
    cities.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv("q8.tsv")


    //Creates the dataframe for question 15: creates a dataframe with the event name and the venue's city, date,
    //city and amount of rsvps received
    val venues = events.select($"name".as("Event Name"), $"v_name".as("Venue Name"),
      $"local_date".as("Date of Event"), $"localized_location".as("City"),
      $"yes_rsvp_count".as("RSVPs received"), $"group_name".as("Group Name"))

    //Returns a dataframe that contains the venue name, the group that hosted the event's name and the location
    //  ot the group
    val popVenues = venues.select($"Venue Name", $"City")
      .filter($"Venue Name" =!= "Online event")
      .groupBy("Venue Name", "City")
      .agg(count("Venue Name").as("Number of Events"))
      .orderBy(functions.desc("Number of Events"))

    //popVenues.show(false)

    //Writes the answer to question 15 to a file.
    popVenues.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv("q15.tsv")

    //Side question which group hosted the most events
    val popGroups = venues.select($"Group Name", $"City")
      .groupBy("Group Name", "City")
      .agg(count("Group Name").as("Number of hosted events"))
      .orderBy(functions.desc("Number of hosted events"))
      .limit(10)
      //.show(false)

    //These are a few queries that show the events from venues that hosted the most events with the
    //most rsvps received.

    val nova = venues.select("*")
      .filter($"Venue Name" === "Nova Labs")
      .orderBy(functions.desc("RSVPs received"))
      .limit(10)
      //.show(false)

    val omni = venues.select("*")
      .filter($"Venue Name" === "Counter Culture Labs, at the Omni Commons")
      .orderBy(functions.desc("RSVPs received"))
      .limit(10)
      //.show(false)

    val corp = venues.select("*")
      .filter($"Venue Name" === "Extensive Business Solutions Corp.")
      .orderBy(functions.desc("RSVPs received"))
      .limit(10)
      //.show(false)

    val plano = venues.select("*")
      .filter($"Venue Name" === "TheLab.ms - Plano's Makerspace")
      .orderBy(functions.desc("RSVPs received"))
      .limit(10)
      //.show(false)

  }
}
