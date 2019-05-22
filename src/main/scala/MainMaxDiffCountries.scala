import model.CountryCityRankCompareItemParser
import operators.SQLQueryBuilder
import org.apache.spark.sql.SparkSession
import queries.MaxDiffCountriesQuery

object MainMaxDiffCountries {

  /**
    * main function
    *
    * @param args input arguments
    */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("JASMINE")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()

    val inputBasePath = "data/inputs/processed/"
    val outputBasePath = "data/outputs/sql/"

    spark.read.parquet(inputBasePath + "parquet/city_attributes.parquet")
      .createOrReplaceTempView("attributes")

    spark.read.parquet(inputBasePath + "parquet/temperature.parquet")
      .createOrReplaceTempView("temperature")
    val temperatureInput = new SQLQueryBuilder(spark, "temperature")
      .sql("temperature", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

    val maxDiffCountriesOutputPath = outputBasePath + "max_diff_countries"
    val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
    maxDiffCountriesOutput.show(false)
    maxDiffCountriesOutput.rdd.map(CountryCityRankCompareItemParser.FromRow).coalesce(1).saveAsTextFile(maxDiffCountriesOutputPath)

    spark.stop()
  }

}
