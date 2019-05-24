package model

import org.apache.spark.sql.types._

/**
  * CityDescriptionItem
  */
object CityDescriptionItem {
  def Schema: StructType = StructType(Array(
    StructField("datetime", StringType, nullable = false),
    StructField("city", StringType, nullable = false),
    StructField("value", StringType, nullable = false)))
}