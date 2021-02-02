package meetup_tests

import org.apache.spark.sql.DataFrame
import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}

import com.cibo.evilplot.colors.HSL
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

object Plots {
  // Plot bar charts using Dataframe results
  def q14_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row =>
      Seq(row(1).toString.toDouble, row(2).toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(_.drop(2))
    BarChart
      .stacked(
        data,
        labels = Seq("online", "in-person")
      )
      .title(s"$title")
      .xAxis(x_labels)
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))

    BarChart
      .stacked(
        data.slice(data.length - 18, data.length),
        labels = Seq("online", "in-person")
      )
      .title(s"$title")
      .xAxis(x_labels.slice(data.length - 18, data.length))
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))
  }

  def q11a_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(_.drop(2))
    BarChart
      .stacked(
        data,
        labels = Seq("free", "cash", "paypal", "wepay")
      )
      .title(s"$title")
      .xAxis(x_labels)
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))
    BarChart
      .stacked(
        data.slice(data.length - 18, data.length),
        labels = Seq("free", "cash", "paypal", "wepay")
      )
      .title(s"$title")
      .xAxis(x_labels.slice(data.length - 18, data.length))
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))
  }

  def q11b_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => row(1).toString.toDouble).toSeq
    val x_labels = df.collect().map(_(0).toString).map(_.drop(2))
    BarChart
      .custom(data.map(Bar.apply))
      .title(s"$title")
      .standard(xLabels = x_labels)
      .render()
      .write(new File(s"${fname}.png"))

    BarChart
      .custom(data.slice(data.length - 18, data.length).map(Bar.apply))
      .title(s"$title")
      .standard(xLabels = x_labels.slice(data.length - 18, data.length))
      .render()
      .write(new File(s"${fname}_zoomed.png"))
  }
}
