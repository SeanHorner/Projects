package MichaelSplaver

import Shared.{Categories, Plotting}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QuestionTwo {
  def questionTwo(spark: SparkSession): Unit = {

    import spark.implicits._

    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", true)
      .option("delimiter", "\t").csv("input/all_cities.tsv")

//    df
//      .withColumn("cat_ids",split($"cat_ids",", "))
//      .select($"cat_ids")
//      .filter(size($"cat_ids") < 1)
//      .show()

    val cat_name = udf[String, Int](Categories.getCategory)

    val catsDf = df
      .withColumn("cat_ids",split($"cat_ids",", "))
      .filter(not(array_contains($"cat_ids","34")))
      .select(explode($"cat_ids").as("cat_id"))
      .withColumn( "cat_name", cat_name($"cat_id"))
      .groupBy($"cat_name")
      .count()
      .orderBy(desc("count"))

//    catsDf
//      .coalesce(1)
//      .write
//      .format("com.databricks.spark.csv")
//      .save("output/question2/categoriesCount")

    Plotting.plotBar(catsDf.limit(5),"Popular Categories Alongside Tech","output/question2/popularCatsTop5.png")

//    df
//      .withColumn("cat_ids",split($"cat_ids",", "))
//      .select($"cat_ids")
//      .groupBy(size($"cat_ids"))
//      .agg(count(size($"cat_ids").as("count")))
//      .show()

  }
}
