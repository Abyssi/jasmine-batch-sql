package model

import org.apache.spark.sql.Row
import utils.JsonSerializable

@SerialVersionUID(100L)
class Metrics(val mean: Double, val stdev: Double, val min: Double, val max: Double) extends Serializable with JsonSerializable {
}

@SerialVersionUID(100L)
class YearMonthCountryMetricsItem(val year: Int, val month: Int, val country: String, val metrics: Metrics) extends Serializable with JsonSerializable {
}

object YearMonthCountryMetricsItemParser {
  def FromTuple(tuple: ((Int, Int, String), (Double, Double, Double, Double))): YearMonthCountryMetricsItem = {
    new YearMonthCountryMetricsItem(tuple._1._1, tuple._1._2, tuple._1._3, new Metrics(tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4))
  }

  def FromRow(row: Row): YearMonthCountryMetricsItem = {
    new YearMonthCountryMetricsItem(row.getInt(0), row.getInt(1), row.getString(2), new Metrics(row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6)))
  }
}
