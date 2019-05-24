package utils

/**
  * Configuration class
  * @param inputBasePath
  * @param outputBasePath
  * @param inputFormat
  * @param needJoin
  * @param clearCitiesQueryEnabled
  * @param countryMetricsQueryEnabled
  * @param maxDiffCountriesQueryEnabled
  */
case class Config(inputBasePath: String, outputBasePath: String, inputFormat: String, needJoin: Boolean, clearCitiesQueryEnabled: Boolean, countryMetricsQueryEnabled: Boolean, maxDiffCountriesQueryEnabled: Boolean)

object Config {
  def parseArgs(args: Array[String]): Config = args.sliding(2, 1).toList.foldLeft(this.default) {
    case (accumArgs, currArgs) => currArgs match {
      case Array("--input-base-path", inputBasePath) => accumArgs.copy(inputBasePath = inputBasePath)
      case Array("--output-base-path", outputBasePath) => accumArgs.copy(outputBasePath = outputBasePath)
      case Array("--input-format", inputFormat) => accumArgs.copy(inputFormat = inputFormat)
      case Array("--need-join", needJoin) => accumArgs.copy(needJoin = needJoin.toBoolean)
      case Array("--clear-cities-query-enabled", clearCitiesQueryEnabled) => accumArgs.copy(clearCitiesQueryEnabled = clearCitiesQueryEnabled.toBoolean)
      case Array("--country-metrics-query-enabled", countryMetricsQueryEnabled) => accumArgs.copy(countryMetricsQueryEnabled = countryMetricsQueryEnabled.toBoolean)
      case Array("--max-diff-countries-query-enabled", maxDiffCountriesQueryEnabled) => accumArgs.copy(maxDiffCountriesQueryEnabled = maxDiffCountriesQueryEnabled.toBoolean)
      case _ => accumArgs // Do whatever you want for this case
    }
  }

  def default = new Config(
    "data/inputs/processed/need-join/",
    "data/outputs/sql/",
    "parquet",
    true,
    true,
    true,
    true
  )
}
