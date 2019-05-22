import model.YearCityItemParser
import operators.SQLQueryBuilder
import org.apache.spark.sql.SparkSession
import queries.ClearCitiesQuery

object MainClearCities {

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

    spark.read.parquet(inputBasePath + "parquet/weather_description.parquet")
      .createOrReplaceTempView("weather_description")
    val weatherDescriptionInput = new SQLQueryBuilder(spark, "weather_description")
      .sql("weather_description", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

    val clearCitiesOutputPath = outputBasePath + "sql/clear_cities"
    val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
    clearCitiesOutput.show()
    clearCitiesOutput.rdd.map(YearCityItemParser.FromRow).saveAsTextFile(clearCitiesOutputPath)

    spark.stop()
  }

}
