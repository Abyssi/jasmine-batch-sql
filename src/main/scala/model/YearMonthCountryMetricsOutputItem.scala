package model

import org.apache.spark.sql.Row
import utils.JsonSerializable

/**
  * Metrics class
  * @param mean
  * @param stdev
  * @param min
  * @param max
  */
case class Metrics(mean: Double, stdev: Double, min: Double, max: Double) extends Serializable with JsonSerializable

/**
  * YearMonthCountryMetricsOutputItem class
  * @param year
  * @param month
  * @param country
  * @param metrics
  */
case class YearMonthCountryMetricsOutputItem(year: Int, month: Int, country: String, metrics: Metrics) extends Serializable with JsonSerializable

object YearMonthCountryMetricsOutputItem {
  def From(tuple: ((Int, Int, String), (Double, Double, Double, Double))): YearMonthCountryMetricsOutputItem = YearMonthCountryMetricsOutputItem(tuple._1._1, tuple._1._2, tuple._1._3, Metrics(tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4))

  def From(row: Row): YearMonthCountryMetricsOutputItem = YearMonthCountryMetricsOutputItem(row.getInt(0), row.getInt(1), row.getString(2), Metrics(row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6)))
}
