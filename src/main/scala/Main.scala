import model.{CountryCityRankCompareItemParser, YearCityItemParser, YearMonthCountryMetricsItemParser}
import operators.SQLQueryBuilder
import org.apache.spark.sql.SparkSession
import queries.{ClearCitiesQuery, CountryMetricsQuery, MaxDiffCountriesQuery}
import utils.{Config, ProfilingUtils}

object Main {

  /**
    * main function
    *
    * @param args input arguments
    */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JASMINE Batch SQL")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()

    val config = Config.parseArgs(args)

    if (config.clearCitiesQueryEnabled || config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
      spark.read.parquet(s"${config.inputBasePath}${config.inputFormat}/city_attributes.${config.inputFormat}")
        .createOrReplaceTempView("attributes")

      // CLEAR CITIES QUERY
      if (config.clearCitiesQueryEnabled) {
        spark.read.parquet(s"${config.inputBasePath}${config.inputFormat}/weather_description.${config.inputFormat}")
          .createOrReplaceTempView("weather_description")
        val weatherDescriptionInput = new SQLQueryBuilder(spark, "weather_description")
          .sql("weather_description", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

        val clearCitiesOutputPath = config.outputBasePath + "sql/clear_cities"
        val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
        ProfilingUtils.timeDataFrame(clearCitiesOutput, "clear Cities Output")

        clearCitiesOutput.show()
        clearCitiesOutput.rdd.map(YearCityItemParser.FromRow).saveAsTextFile(clearCitiesOutputPath)
      }

      if (config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
        spark.read.parquet(s"${config.inputBasePath}${config.inputFormat}/temperature.${config.inputFormat}")
          .createOrReplaceTempView("temperature")
        val temperatureInput = new SQLQueryBuilder(spark, "temperature")
          .sql("temperature", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

        // COUNTRY METRICS QUERY
        if (config.countryMetricsQueryEnabled) {
          spark.read.parquet(s"${config.inputBasePath}${config.inputFormat}/humidity.${config.inputFormat}")
            .createOrReplaceTempView("humidity")
          val humidityInput = new SQLQueryBuilder(spark, "humidity")
            .sql("humidity", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

          spark.read.parquet(s"${config.inputBasePath}${config.inputFormat}/humidity.${config.inputFormat}")
            .createOrReplaceTempView("humidity")
          val pressureInput = new SQLQueryBuilder(spark, "pressure")
            .sql("pressure", "SELECT TO_TIMESTAMP(REPLACE(table.datetime, ' ', 'T') || attributes.timeOffset, \"yyyy-MM-dd'T'HH:mm:ssZ\") as datetime, table.city, attributes.country, table.value FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

          val humidityCountryMetricsOutputPath = config.outputBasePath + "humidity_country_metrics"
          val humidityCountryMetricsOutput = CountryMetricsQuery.run(humidityInput)
          humidityCountryMetricsOutput.show()
          humidityCountryMetricsOutput.rdd.map(YearMonthCountryMetricsItemParser.FromRow).saveAsTextFile(humidityCountryMetricsOutputPath)

          val pressureCountryMetricsOutputPath = config.outputBasePath + "pressure_country_metrics"
          val pressureCountryMetricsOutput = CountryMetricsQuery.run(pressureInput)
          pressureCountryMetricsOutput.show()
          pressureCountryMetricsOutput.rdd.map(YearMonthCountryMetricsItemParser.FromRow).saveAsTextFile(pressureCountryMetricsOutputPath)

          val temperatureCountryMetricsOutputPath = config.outputBasePath + "temperature_country_metrics"
          val temperatureCountryMetricsOutput = CountryMetricsQuery.run(temperatureInput)
          temperatureCountryMetricsOutput.show(false)
          temperatureCountryMetricsOutput.rdd.map(YearMonthCountryMetricsItemParser.FromRow).saveAsTextFile(temperatureCountryMetricsOutputPath)
        }

        // MAX DIFF COUNTRIES QUERY
        if (config.maxDiffCountriesQueryEnabled) {
          val maxDiffCountriesOutputPath = config.outputBasePath + "max_diff_countries"
          val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
          maxDiffCountriesOutput.show(false)
          maxDiffCountriesOutput.rdd.map(CountryCityRankCompareItemParser.FromRow).saveAsTextFile(maxDiffCountriesOutputPath)
        }
      }
    }

    System.in.read
    spark.stop()
  }

}
