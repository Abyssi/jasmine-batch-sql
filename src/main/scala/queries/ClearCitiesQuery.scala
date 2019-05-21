package queries

import operators.SQLQueryBuilder
import org.apache.spark.sql.DataFrame

/**
  * Query 1
  * Identify the cities with clean sky, all day long,
  * for at least 15 days a month in March, April and May
  * for each year.
  **/
object ClearCitiesQuery {

  def run(input: SQLQueryBuilder): DataFrame = {
    input
      .sql("filtered", "SELECT * FROM {TABLE_NAME} WHERE value='sky is clear' AND MONTH(datetime) IN (3, 4, 5)")
      .sql("aggregation_ymdc", "SELECT YEAR(datetime) AS year, MONTH(datetime) AS month, DAY(datetime) AS day, city AS city, COUNT(*) AS count FROM {TABLE_NAME} GROUP BY YEAR(datetime), MONTH(datetime), DAY(datetime), city")
      .sql("filtered_ymdc", "SELECT year, month, day, city FROM {TABLE_NAME} WHERE count >= 20")
      .sql("aggregation_ymc", "SELECT year, month, city, COUNT(*) AS count FROM {TABLE_NAME} GROUP BY year, month, city")
      .sql("filtered_ymc", "SELECT year, month, city FROM {TABLE_NAME} WHERE count >= 15")
      .sql("aggregation_yc", "SELECT year, city, COUNT(*) AS count FROM {TABLE_NAME} GROUP BY year, city")
      .sql("filtered_yc", "SELECT year, city FROM {TABLE_NAME} WHERE count == 3")
      .collect()
  }

}
