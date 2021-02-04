package Shared

import com.cibo.evilplot.colors._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

object Plotting {

  /**
   * Plot normal bar chart
   * @param df Takes in dataframe results from Spark SQL.
   * @param title Name of the chart.
   * @param fullPath Full file path for location to save image
   */
  def plotBar(df: DataFrame, title: String, fullPath: String): Unit ={
    // Convert dataframe into sequence of doubles for axes
    val seq = df.collect().map(row => row.toSeq.map(_.toString)).toSeq.flatten

    var yAxis = Seq[Double]()
    var xAxis = Seq[String]()

    // Populate x-axis and y-axis
    for(i <- 0 until seq.length){
      if(i%2 == 0){
        xAxis = xAxis :+ (seq(i))
      }
    }
    for(i <- 0 until seq.length){
      if(i%2 != 0){
        yAxis = yAxis :+ seq(i).toDouble
      }
    }
    // Plot a bar chart
    BarChart.custom(yAxis.map(Bar.apply), spacing = Some(10))
      .title(title)
      .standard(xLabels = xAxis)
      .ybounds(lower = 0)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(fullPath))
  }

}
