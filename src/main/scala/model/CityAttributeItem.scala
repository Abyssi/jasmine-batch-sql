package model

import org.apache.spark.sql.types._

/**
  * CityAttributeItem object
  */
object CityAttributeItem {
  def Schema: StructType = StructType(Array(
    StructField("City", StringType, nullable = false),
    StructField("country", StringType, nullable = false),
    StructField("timezone", StringType, nullable = false)))
}