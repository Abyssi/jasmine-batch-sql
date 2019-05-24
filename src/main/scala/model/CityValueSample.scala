package model

import org.apache.spark.sql.types._

/**
  * City Value Sample object
  */
object CityValueSample {
  def Schema: StructType = StructType(Array(
    StructField("datetime", TimestampType, nullable = false),
    StructField("timezone", StringType, nullable = false),
    StructField("city", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false)))
}