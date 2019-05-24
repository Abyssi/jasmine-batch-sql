import java.util.TimeZone

import connectors.FormatReader
import model._
import operators.SQLQueryBuilder
import org.apache.spark.sql.SparkSession
import queries.{ClearCitiesQuery, CountryMetricsQuery, MaxDiffCountriesQuery}
import utils.Config

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

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val config = Config.parseArgs(args)

    if (config.clearCitiesQueryEnabled || config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
      if (config.needJoin) FormatReader.read(spark, config.inputFormat, CityAttributeItem.Schema, s"${config.inputBasePath}${config.inputFormat}/city_attributes.${config.inputFormat}")
        .createOrReplaceTempView("attributes")

      // CLEAR CITIES QUERY
      if (config.clearCitiesQueryEnabled) {
        FormatReader.read(spark, config.inputFormat, if (config.needJoin) CityDescriptionItem.Schema else CityDescriptionSample.Schema, s"${config.inputBasePath}${config.inputFormat}/weather_description.${config.inputFormat}")
          .createOrReplaceTempView("weather_description")
        var weatherDescriptionInput = new SQLQueryBuilder(spark, "weather_description")
        if (config.needJoin)
          weatherDescriptionInput = weatherDescriptionInput.sql("weather_description", "SELECT from_utc_timestamp(table.datetime, attributes.timezone) AS datetime, table.city, attributes.country, CAST(table.value AS STRING) FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

        val clearCitiesOutputPath = config.outputBasePath + "clear_cities"
        val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
        //ProfilingUtils.timeDataFrame(clearCitiesOutput, "clear Cities Output")
        //clearCitiesOutput.show()
        clearCitiesOutput.rdd.map(YearCityOutputItem.From).saveAsTextFile(clearCitiesOutputPath)
      }

      if (config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
        FormatReader.read(spark, config.inputFormat, if (config.needJoin) CityValueItem.Schema else CityCountryValueSample.Schema, s"${config.inputBasePath}${config.inputFormat}/temperature.${config.inputFormat}")
          .createOrReplaceTempView("temperature")
        var temperatureInput = new SQLQueryBuilder(spark, "temperature")
        if (config.needJoin)
          temperatureInput = temperatureInput.sql("temperature", "SELECT from_utc_timestamp(table.datetime, attributes.timezone) AS datetime, table.city, attributes.country, CAST(table.value AS DOUBLE) FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

        // COUNTRY METRICS QUERY
        if (config.countryMetricsQueryEnabled) {
          FormatReader.read(spark, config.inputFormat, if (config.needJoin) CityValueItem.Schema else CityCountryValueSample.Schema, s"${config.inputBasePath}${config.inputFormat}/humidity.${config.inputFormat}")
            .createOrReplaceTempView("humidity")
          var humidityInput = new SQLQueryBuilder(spark, "humidity")
          if (config.needJoin)
            humidityInput = humidityInput.sql("humidity", "SELECT from_utc_timestamp(table.datetime, attributes.timezone) AS datetime, table.city, attributes.country, CAST(table.value AS DOUBLE) FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

          FormatReader.read(spark, config.inputFormat, if (config.needJoin) CityValueItem.Schema else CityCountryValueSample.Schema, s"${config.inputBasePath}${config.inputFormat}/pressure.${config.inputFormat}")
            .createOrReplaceTempView("pressure")
          var pressureInput = new SQLQueryBuilder(spark, "pressure")
          if (config.needJoin)
            pressureInput = pressureInput.sql("pressure", "SELECT from_utc_timestamp(table.datetime, attributes.timezone) AS datetime, table.city, attributes.country, CAST(table.value AS DOUBLE) FROM {TABLE_NAME} AS table INNER JOIN attributes ON table.city=attributes.City")

          val humidityCountryMetricsOutputPath = config.outputBasePath + "humidity_country_metrics"
          val humidityCountryMetricsOutput = CountryMetricsQuery.run(humidityInput)
          //humidityCountryMetricsOutput.show()
          humidityCountryMetricsOutput.rdd.map(YearMonthCountryMetricsOutputItem.From).saveAsTextFile(humidityCountryMetricsOutputPath)

          val pressureCountryMetricsOutputPath = config.outputBasePath + "pressure_country_metrics"
          val pressureCountryMetricsOutput = CountryMetricsQuery.run(pressureInput)
          //pressureCountryMetricsOutput.show()
          pressureCountryMetricsOutput.rdd.map(YearMonthCountryMetricsOutputItem.From).saveAsTextFile(pressureCountryMetricsOutputPath)

          val temperatureCountryMetricsOutputPath = config.outputBasePath + "temperature_country_metrics"
          val temperatureCountryMetricsOutput = CountryMetricsQuery.run(temperatureInput)
          //temperatureCountryMetricsOutput.show(false)
          temperatureCountryMetricsOutput.rdd.map(YearMonthCountryMetricsOutputItem.From).saveAsTextFile(temperatureCountryMetricsOutputPath)
        }

        // MAX DIFF COUNTRIES QUERY
        if (config.maxDiffCountriesQueryEnabled) {
          val maxDiffCountriesOutputPath = config.outputBasePath + "max_diff_countries"
          val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
          //maxDiffCountriesOutput.show(false)
          maxDiffCountriesOutput.rdd.map(CountryCityRankCompareOutputItem.From).saveAsTextFile(maxDiffCountriesOutputPath)
        }
      }
    }

    //System.in.read
    spark.stop()
  }

}
