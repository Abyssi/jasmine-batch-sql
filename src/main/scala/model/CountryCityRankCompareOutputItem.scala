package model

import org.apache.spark.sql.Row
import utils.JsonSerializable

/**
  * CountryCityRankItem class
  * @param position
  * @param value
  */
case class CountryCityRankItem(position: Int, value: Double) extends Serializable with JsonSerializable

/**
  * CountryCityRankCompareOutputItem
  * @param country
  * @param city
  * @param newRank
  * @param oldRank
  */
case class CountryCityRankCompareOutputItem(country: String, city: String, newRank: CountryCityRankItem, oldRank: CountryCityRankItem) extends Serializable with JsonSerializable

object CountryCityRankCompareOutputItem {
  def From(tuple: ((String, String), ((Int, Double), (Int, Double)))): CountryCityRankCompareOutputItem = CountryCityRankCompareOutputItem(tuple._1._1, tuple._1._2, CountryCityRankItem(tuple._2._1._1, tuple._2._1._2), CountryCityRankItem(tuple._2._2._1, tuple._2._2._2))

  def From(row: Row): CountryCityRankCompareOutputItem = CountryCityRankCompareOutputItem(row.getString(0), row.getString(1), CountryCityRankItem(row.getInt(2), row.getDouble(3)), CountryCityRankItem(row.getInt(4), row.getDouble(5)))
}
