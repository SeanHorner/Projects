import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object AnalysisTesting {

  def main(args: Array[String]) = {

    val tsvFile = "data_50cities_v2.tsv" //Change this to proper path for data

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("AnalysisTesting")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawEventDF = spark.read.option("sep","\t").option("header","true").csv(tsvFile)
    rawEventDF.show(20)

    val eventDF = rawEventDF.withColumn("yes_rsvp_count",col("yes_rsvp_count").cast(IntegerType))
    //Getting the highest RSVP count
    eventDF.select("name","yes_rsvp_count")
      .sort($"yes_rsvp_count".desc)
      .show(20)
  }


}
