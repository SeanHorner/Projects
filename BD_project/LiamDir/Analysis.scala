package meetup_tests

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Analysis {
  def online_event_count_trend(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val new_data_online = data
      .select($"date_month" as "date_month_temp", $"is_online_event", $"status")
      .where($"status" === "past")
      .where($"is_online_event" === true)
      .groupBy($"date_month_temp")
      .count()
      .withColumnRenamed("count", "online_count")

    val new_data_off = data
      .select($"date_month", $"is_online_event", $"status")
      .where($"status" === "past")
      .where($"is_online_event" === false)
      .groupBy($"date_month")
      .count()
      .withColumnRenamed("count", "inperson_count")
    val new_data = new_data_online

      .join(new_data_off,  $"date_month" === $"date_month_temp", joinType = "full_outer")
      .where($"date_month" < "2021-02")
      .na.fill(0, Seq("inperson_count", "online_count"))
      .select($"date_month", $"online_count", $"inperson_count")
      .sort("date_month")

    new_data.show()
    new_data.printSchema()
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
    val ness_data = data
      .select($"date_month", $"accepts", $"status")
      .na.fill("free", Array("accepts"))
      .where($"status" === "past")
      .where($"date_month" < "2021-02")
      .drop($"status")

    val free_data = ness_data
      .where($"accepts" === "free")
      .groupBy($"date_month")
      .agg(count("accepts") as "free")
    val cash_data = ness_data
      .where($"accepts" === "cash")
      .groupBy($"date_month" as "cash_date")
      .agg(count("accepts") as "cash")
    val paypal_data = ness_data
      .where($"accepts" === "paypal")
      .groupBy($"date_month" as "paypal_date")
      .agg(count("accepts") as "paypal")
    val wepay_data = ness_data
      .where($"accepts" === "wepay")
      .groupBy($"date_month" as "wepay_date")
      .agg(count("accepts") as "wepay")

    val type_data = free_data
      .join(cash_data, $"date_month" === $"cash_date", "full_outer")
      .join(paypal_data, $"date_month" === $"paypal_date", "full_outer")
      .join(wepay_data, $"date_month" === $"wepay_date", "full_outer")
      .na.fill(0, Seq("free", "cash", "paypal", "wepay"))

    type_data
      .select($"date_month", $"free", $"cash", $"paypal", $"wepay")
      .sort(asc("date_month"))
  }

  def fee_amount(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val ness_data = data
      .select($"date_month", $"amount", $"status")
      .na.fill(0, Array("amount"))
      .where($"status" === "past")
      .where($"date_month" < "2021-02")
      .drop($"status")

    val type_data = ness_data
      .groupBy($"date_month")
      .agg(round(avg($"amount".cast(DoubleType)), 2) as "average_fee")
      .where($"average_fee" > 0)

    type_data.sort(asc("date_month"))
  }

  def topic_trend(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val topic_list = data
      .select(explode($"category_ids") as "topic")
      .groupBy("topic")
      .count()
      .sort(desc("count"))
      .head(10)
      .map(_(0).toString())
      .toList
    for (topic <- topic_list){
      println(topic)
    }

    data
      .select($"date_month", explode($"category_ids") as "topic")
      .groupBy($"date_month", $"topic")
      .count()
      .sort(desc("date_month"), desc("count"), asc("topic"))
  }

  def date_to_month(date: String): String = {
    date.replaceFirst("(\\d+-\\d+)-\\d+", "$1")
  }

}
