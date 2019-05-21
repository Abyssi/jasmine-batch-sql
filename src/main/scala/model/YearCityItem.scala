package model

import org.apache.spark.sql.Row
import utils.JsonSerializable

@SerialVersionUID(100L)
class YearCityItem(val year: Int, val city: String) extends Serializable with JsonSerializable {
}

object YearCityItemParser {
  def FromTuple(tuple: (Int, String)): YearCityItem = {
    new YearCityItem(tuple._1, tuple._2)
  }

  def FromRow(row: Row): YearCityItem = {
    new YearCityItem(row.getInt(0), row.getString(1))
  }
}
