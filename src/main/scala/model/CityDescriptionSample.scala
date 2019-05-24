package model

import org.apache.spark.sql.types._

/**
  * CityDescriptionSample
  */
object CityDescriptionSample {
  def Schema: StructType = StructType(Array(
    StructField("datetime", TimestampType, nullable = false),
    StructField("timezone", StringType, nullable = false),
    StructField("city", StringType, nullable = false),
    StructField("value", StringType, nullable = false)))
}