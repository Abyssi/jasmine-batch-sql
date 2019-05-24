package connectors

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Format reader
  */
object FormatReader {
  def read(spark: SparkSession, format: String, schema: StructType, path: String): DataFrame = {
    format match {
      case "parquet" => spark.read.parquet(path)
      case "csv" => spark.read.option("header", value = true).schema(schema).csv(path)
      case _ => throw new IllegalArgumentException
    }
  }
}
