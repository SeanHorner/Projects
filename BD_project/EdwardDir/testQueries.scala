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

    //This dataframe takes in the json (for now) and is used to pull out the venue field.
    val eventsjson = spark.read.option("multiline", "true").json("data_50cities_array.json")

   //This is a dataframe containing the name, city, address, and state of the venue of each event and is ordered
   //by the amount of events that are held in each venue (including online event)
    val venues = eventsjson.select($"venue.name".as("Venue Name"), $"venue.city".as("City"), $"venue.address_1".as("Address"),
     $"venue.state".as("State"))
      .filter($"venue.name" =!= "NULL")
      .groupBy("Venue Name", "City", "Address", "State")
      .agg(count("Venue Name").as("Number of events"))
      .orderBy(functions.desc("Number of events"))
      .show(false)

   // venues.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv("q15.tsv")

    //Creates a dataframe that will contain the event name, the amount of yes RSVPs for the event, and the venue's name, city, and address
    //  The dataframe is made using the dataframe with the json (for now)
    val eventsWithRSVPs = eventsjson.select($"name".as("Event Name"), $"venue.name".as("Venue Name"), $"venue.city".as("City"), $"venue.address_1".as("Address"),
      $"venue.state".as("State"), $"rsvp_count".as("RSVPs received"), $"local_date".as("Date of Event"))
      .filter($"venue.name" =!= "NULL")
      .orderBy(functions.desc("RSVPs received"))

    eventsWithRSVPs.show(false)

    //Returns the online events with the most yes rsvps
    val onlineEvents = eventsWithRSVPs.select($"Event Name", $"Venue Name", $"RSVPs received", $"Date of Event").distinct()
      .filter($"Venue Name" === "Online event")
      .orderBy(functions.desc("RSVPs received"))
      .show(false)

    //Returns the events from the Newton Free Library with the most RSVPs
    val libraryEvents = eventsWithRSVPs.select("*").distinct()
      .filter($"Venue Name" === "Newton Free Library")
      .orderBy(functions.desc("RSVPs received"))
      .show(false)

    //Returns the events from the Microsoft NERD center with the most RSVPS
    val microsoftEvents = eventsWithRSVPs.select("*").distinct()
      .filter($"Venue Name" === "Microsoft New England Research & Development Center (NERD)")
      .orderBy(functions.desc("RSVPs received"))
      .show(false)

    //Returns the events from FUBAR Labs with the most RSVPs
    val fubarEvents = eventsWithRSVPs.select("*").distinct()
      .filter($"Venue Name" === "FUBAR Labs")
      .orderBy(functions.desc("RSVPs received"))
      .show(false)

    //Returns the events from Practical Programming with the most RSVPs
    val practicalEvents = eventsWithRSVPs.select("*").distinct()
      .filter($"Venue Name" === "Practical Programming")
      .orderBy(functions.desc("RSVPs received"))
      .show(false)

    //Returns the events from MIT CSAIL with the most RSVPs
    val mitEvents = eventsWithRSVPs.select("*").distinct()
      .filter($"Venue Name" === "MIT CSAIL")
      .orderBy(functions.desc("RSVPs received"))
      .show(false)

    //This dataframe is created using the current tsv
    val events = spark.read.option("sep", "\t").option("header", "true").csv("data_50cities_v2.tsv")
      .withColumn("rsvp_count", $"yes_rsvp_count".cast("Int"))

    //This dataframe just has the city (localized_location) and a column containing how many events were
    //held in the city.
    val cities = events.select($"localized_location".as("city"))
      .groupBy("city")
      .agg(count("city").as("number of events"))
      .sort(functions.desc("number of events"))

    cities.show(false)

    //cities.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv("q8.tsv")



  }
}
