package meetup_tests

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Analysis {
  def online_event_count_trend(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val new_data_online = data
      .select($"local_date", $"is_online_event", $"status")
      .where($"status" === "past")
      .where($"is_online_event" === true)
      .groupBy($"local_date")
      .count()
      .withColumnRenamed("count", "online_count")
    val new_data_off = data
      .select($"local_date", $"is_online_event", $"status")
      .where($"status" === "past")
      .where($"is_online_event" === false)
      .groupBy($"local_date")
      .count()
      .withColumnRenamed("count", "inperson_count")
    val new_data = data.select($"local_date", $"status")
      .where($"status" === "past")
      .groupBy($"local_date").count()
      .withColumnRenamed("count", "total_events")
      .join(new_data_online, "local_date")
      .join(new_data_off, "local_date")
      .sort(desc("local_date"))

    new_data
  }

  def event_per_group(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val new_data = data
      .select($"group_name")
      .groupBy($"group_name")
      .count()
      .withColumnRenamed("count", "total_events")
      .sort(desc("total_events"))

    new_data
  }

  def fee(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
data.na
    val ness_data = data
      .select($"date_month", $"accepts", $"amount", $"status")
      .na.fill("free", Array("accepts"))
      .na.fill("0", Array("amount"))
      .where($"status" === "past")
      .drop($"status")

    ness_data.where($"accepts" === "free").show()

    val type_data = ness_data
      .groupBy($"date_month", $"accepts")
      .agg(count($"amount") as "count", round(avg($"amount".cast(DoubleType)), 2) as "average_fee")

    type_data.sort(asc("accepts"), desc("date_month"))
  }

  def date_to_month(date: String): String = {
    date.replaceFirst("(\\d+-\\d+)-\\d+", "$1")
  }
}
