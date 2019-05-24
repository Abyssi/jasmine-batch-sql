package model

import org.apache.spark.sql.types._

/**
  * CityValueItem object
  */
object CityValueItem {
  def Schema: StructType = StructType(Array(
    StructField("datetime", StringType, nullable = false),
    StructField("city", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false)))
}