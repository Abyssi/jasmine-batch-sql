package queries

import operators.SQLQueryBuilder
import org.apache.spark.sql.DataFrame

/**
  * Query 2
  **/
object CountryMetricsQuery {

  def run(input: SQLQueryBuilder): DataFrame = {
    input
      .sql("aggregation_ymC", "SELECT YEAR(datetime) AS year, MONTH(datetime) AS month, country AS country, AVG(value) AS avg, STDDEV(value) stdev, MIN(value) AS min, MAX(value) AS max FROM {TABLE_NAME} GROUP BY YEAR(datetime), MONTH(datetime), country")
      .collect()
  }
}
