package meetup_tests

import org.apache.spark.sql.DataFrame
import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}

import com.cibo.evilplot.colors.{Color, HSL}
import com.cibo.evilplot.numeric.Point
import com.cibo.evilplot.plot.LinePlot._
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.renderers.PathRenderer

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

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

  def q14_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString)
    val months = x_labels.map(date_month_to_double(_))
    val colors = Color.getGradientSeq(2)

    val points: ListBuffer[Seq[Point]] = ListBuffer()
    for (ii <- 0 to data(0).length-1) {
      points.append(months.zip(data.map(_(ii)))
        .toSeq
        .map(row => Point(row._1, row._2)))
    }

    Overlay(
      LinePlot.series(points(0), "Online", colors(0)),
      LinePlot.series(points(1), "In Person", colors(1))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))

    Overlay(
      LinePlot.series(points(0).slice(points(0).length - 24, points(0).length), "Online", colors(0)),
      LinePlot.series(points(1).slice(points(0).length - 24, points(0).length), "In Person", colors(1))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))
  }

  def q11a_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2), row(3), row(4)).map(_.toString.toDouble))
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
        data.map(row => Seq(row(1), row(2), row(3))),
        labels = Seq("cash", "paypal", "wepay")
      )
      .title(s"$title")
      .xAxis(x_labels)
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_nf.png"))

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

    BarChart
      .stacked(
        data.slice(data.length - 18, data.length).map(row => Seq(row(1), row(2), row(3))),
        labels = Seq("cash", "paypal", "wepay")
      )
      .title(s"$title")
      .xAxis(x_labels.slice(data.length - 18, data.length))
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_nf_zoomed.png"))
  }

  def q11a_line_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2), row(3), row(4)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
//    val x_labels = months_temp.map(_ - months_temp.min)
    val colors = Color.getGradientSeq(4)
    val labels = Seq("free", "cash", "paypal", "wepay")

    val points: ListBuffer[Seq[Point]] = ListBuffer()
    for (ii <- 0 to data(0).length-1) {
      points.append(x_labels.zip(data.map(_(ii)))
        .toSeq
        .map(row => Point(row._1, row._2)))
    }

    Overlay(
      LinePlot.series(points(0), labels(0), colors(0)),
      LinePlot.series(points(1), labels(1), colors(1)),
      LinePlot.series(points(2), labels(2), colors(2)),
      LinePlot.series(points(3), labels(3), colors(3))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))

    Overlay(
      LinePlot.series(points(1), labels(1), colors(1)),
      LinePlot.series(points(2), labels(2), colors(2)),
      LinePlot.series(points(3), labels(3), colors(3))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_nf.png"))
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

  def q11b_line_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2), row(3), row(4)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val colors = Color.getGradientSeq(4)
    val labels = Seq("Cash", "PayPal", "WePay", "Average")

    val points: ListBuffer[Seq[Point]] = ListBuffer()
    for (ii <- 0 to data(0).length-1) {
      points.append(x_labels.zip(data.map(_(ii)))
        .toSeq
        .map(row => Point(row._1, row._2)))
    }

    Overlay(
      LinePlot.series(points(0).slice(points(0).length - 24, points(0).length), labels(0), colors(0)),
      LinePlot.series(points(1).slice(points(0).length - 24, points(0).length), labels(1), colors(1)),
      LinePlot.series(points(2).slice(points(0).length - 24, points(0).length), labels(2), colors(2)),
      LinePlot.series(points(3).slice(points(0).length - 24, points(0).length), labels(3), colors(3))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))

    Overlay(
      LinePlot.series(points(0), labels(0), colors(0)),
      LinePlot.series(points(1), labels(1), colors(1)),
      LinePlot.series(points(2), labels(2), colors(2)),
      LinePlot.series(points(3), labels(3), colors(3))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))
  }

  def q7_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(3), row(4), row(5), row(6), row(7), row(8)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString)

    BarChart
      .stacked(
        data,
        labels = Seq("Career & Business (2)", "Education (6)", "Movements (4)", "Hobbies & Crafts(15)", "Writing (36)")
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
        labels = Seq("Career & Business (2)", "Education (6)", "Movements (4)", "Hobbies & Crafts(15)", "Writing (36)")
      )
      .title(s"$title")
      .xAxis(x_labels.slice(data.length - 18, data.length))
      .yAxis()
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))
  }

  def q7_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(3), row(4), row(5), row(7), row(8)).map(_.toString.toDouble))
    val months_temp = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val x_labels = months_temp.map(_ - months_temp.min)
    val colors = Color.getGradientSeq(5)
    val labels = Seq("Career & Business (2)", "Education (6)", "Movements (4)", "Hobbies & Crafts(15)", "Writing (36)")

    val points: ListBuffer[Seq[Point]] = ListBuffer()
    for (ii <- 0 to data(0).length-1) {
      points.append(x_labels.zip(data.map(_(ii)))
        .toSeq
        .map(row => Point(row._1, row._2)))
    }

    Overlay(
      LinePlot.series(points(0), labels(0), colors(0)),
      LinePlot.series(points(1), labels(1), colors(1)),
      LinePlot.series(points(2), labels(2), colors(2)),
      LinePlot.series(points(3), labels(3), colors(3)),
      LinePlot.series(points(4), labels(4), colors(4))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))

    Overlay(
      LinePlot.series(points(0).slice(points(0).length - 24, points(0).length), labels(0), colors(0)),
      LinePlot.series(points(1).slice(points(0).length - 24, points(0).length), labels(1), colors(1)),
      LinePlot.series(points(2).slice(points(0).length - 24, points(0).length), labels(2), colors(2)),
      LinePlot.series(points(3).slice(points(0).length - 24, points(0).length), labels(3), colors(3)),
      LinePlot.series(points(4).slice(points(0).length - 24, points(0).length), labels(4), colors(4))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))
    }

  def q7_ny_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(3), row(4), row(5)).map(_.toString.toDouble))
    val months_temp = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val x_labels = months_temp.map(_ - months_temp.min)
    val colors = Color.getGradientSeq(3)
    val labels = Seq("LGBTQ (12)", "Career & Business (2)", "Education (6)")


    val points: ListBuffer[Seq[Point]] = ListBuffer()
    for (ii <- 0 to data(0).length-1) {
      points.append(x_labels.zip(data.map(_(ii)))
        .toSeq
        .map(row => Point(row._1, row._2)))
    }

    Overlay(
      LinePlot.series(points(0), labels(0), colors(0)),
      LinePlot.series(points(1), labels(1), colors(1)),
      LinePlot.series(points(2), labels(2), colors(2))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}.png"))

    Overlay(
      LinePlot.series(points(0).slice(points(0).length - 24, points(0).length), labels(0), colors(0)),
      LinePlot.series(points(1).slice(points(0).length - 24, points(0).length), labels(1), colors(1)),
      LinePlot.series(points(2).slice(points(0).length - 24, points(0).length), labels(2), colors(2))
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .hline(2d)
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${fname}_zoomed.png"))
  }

  def date_month_to_double(date: String): Double = {
    val pattern: Regex = "(\\d+)-(\\d+)".r
    date match {
      case pattern(year, month) if year == "2021" => 0
      case pattern(year, month) if year < "2021" => ((year.toInt - 2020)*12 + (month.toInt - 12) - 1).toDouble
      case _ => 100
    }
  }
}
