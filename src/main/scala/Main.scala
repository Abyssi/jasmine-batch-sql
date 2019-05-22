import model.{CountryCityRankCompareItemParser, YearCityItemParser, YearMonthCountryMetricsItemParser}
import operators.SQLQueryBuilder
import org.apache.spark.sql.SparkSession
import queries.{ClearCitiesQuery, CountryMetricsQuery, MaxDiffCountriesQuery}

object Main {

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

    spark.read.parquet(inputBasePath + "parquet/humidity.parquet")
      .createOrReplaceTempView("humidity")
    val humidityInput = new SQLQueryBuilder(spark, "humidity")
      .sql("humidity", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

    spark.read.parquet(inputBasePath + "parquet/pressure.parquet")
      .createOrReplaceTempView("pressure")
    val pressureInput = new SQLQueryBuilder(spark, "pressure")
      .sql("pressure", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

    spark.read.parquet(inputBasePath + "parquet/temperature.parquet")
      .createOrReplaceTempView("temperature")
    val temperatureInput = new SQLQueryBuilder(spark, "temperature")
      .sql("temperature", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

    spark.read.parquet(inputBasePath + "parquet/weather_description.parquet")
      .createOrReplaceTempView("weather_description")
    val weatherDescriptionInput = new SQLQueryBuilder(spark, "weather_description")
      .sql("weather_description", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

    val clearCitiesOutputPath = outputBasePath + "sql/clear_cities"
    val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
    clearCitiesOutput.show()
    clearCitiesOutput.rdd.map(YearCityItemParser.FromRow).saveAsTextFile(clearCitiesOutputPath)

    val humidityCountryMetricsOutputPath = outputBasePath + "humidity_country_metrics"
    val humidityCountryMetricsOutput = CountryMetricsQuery.run(humidityInput)
    humidityCountryMetricsOutput.show()
    humidityCountryMetricsOutput.rdd.map(YearMonthCountryMetricsItemParser.FromRow).saveAsTextFile(humidityCountryMetricsOutputPath)

    val pressureCountryMetricsOutputPath = outputBasePath + "pressure_country_metrics"
    val pressureCountryMetricsOutput = CountryMetricsQuery.run(pressureInput)
    pressureCountryMetricsOutput.show()
    pressureCountryMetricsOutput.rdd.map(YearMonthCountryMetricsItemParser.FromRow).saveAsTextFile(pressureCountryMetricsOutputPath)

    val temperatureCountryMetricsOutputPath = outputBasePath + "temperature_country_metrics"
    val temperatureCountryMetricsOutput = CountryMetricsQuery.run(temperatureInput)
    temperatureCountryMetricsOutput.show(false)
    temperatureCountryMetricsOutput.rdd.map(YearMonthCountryMetricsItemParser.FromRow).saveAsTextFile(temperatureCountryMetricsOutputPath)

    val maxDiffCountriesOutputPath = outputBasePath + "max_diff_countries"
    val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
    maxDiffCountriesOutput.show(false)
    maxDiffCountriesOutput.rdd.map(CountryCityRankCompareItemParser.FromRow).saveAsTextFile(maxDiffCountriesOutputPath)

    spark.stop()
  }

}
