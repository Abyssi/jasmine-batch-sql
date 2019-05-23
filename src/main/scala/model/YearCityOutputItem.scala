package model

import org.apache.spark.sql.Row
import utils.JsonSerializable

case class YearCityOutputItem(year: Int, city: String) extends Serializable with JsonSerializable

object YearCityOutputItem {
  def From(tuple: (Int, String)): YearCityOutputItem = YearCityOutputItem(tuple._1, tuple._2)

  def From(row: Row): YearCityOutputItem = YearCityOutputItem(row.getInt(0), row.getString(1))
}
