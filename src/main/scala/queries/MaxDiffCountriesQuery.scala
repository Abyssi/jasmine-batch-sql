package queries

import operators.SQLQueryBuilder
import org.apache.spark.sql.DataFrame

/**
  * Query 3
  **/
object MaxDiffCountriesQuery {

  def run(input: SQLQueryBuilder): DataFrame = {
    val middle = input
      .sql("filtered", "SELECT * FROM {TABLE_NAME} WHERE HOUR(datetime) >= 12 AND HOUR(datetime) <= 15 AND YEAR(datetime) IN (2016, 2017) AND CASE MONTH(datetime) WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 WHEN 4 THEN 1 WHEN 5 THEN 0 WHEN 6 THEN 2 WHEN 7 THEN 2 WHEN 8 THEN 2 WHEN 9 THEN 2 WHEN 10 THEN 0 WHEN 11 THEN 0 WHEN 12 THEN 0 END != 0")
      .sql("aggregation_ycCm", "SELECT YEAR(datetime) AS year, city AS city, country AS country, AVG(value) AS avg FROM {TABLE_NAME} GROUP BY YEAR(datetime), city, country, CASE MONTH(datetime) WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 WHEN 4 THEN 1 WHEN 5 THEN 0 WHEN 6 THEN 2 WHEN 7 THEN 2 WHEN 8 THEN 2 WHEN 9 THEN 2 WHEN 10 THEN 0 WHEN 11 THEN 0 WHEN 12 THEN 0 END")
      .sql("aggregation_ycC", "SELECT year, city, country, ABS(MAX(avg) - MIN(avg)) AS value FROM {TABLE_NAME} GROUP BY year, city, country")

    middle
      .sql("sort_2016", "SELECT city, country, ROW_NUMBER() OVER (PARTITION BY country ORDER BY value DESC) AS index, value FROM {TABLE_NAME} WHERE year == 2016")

    middle
      .sql("top_2017", "SELECT city, country, ROW_NUMBER() OVER (PARTITION BY country ORDER BY value DESC) AS index, value FROM {TABLE_NAME} WHERE year == 2017")
      .sql("top_2017", "SELECT * FROM {TABLE_NAME} WHERE index <= 3")
      .sql("join", "SELECT top_2017.country AS country, top_2017.city AS city, top_2017.index AS index2017, top_2017.value AS value2017, sort_2016.index AS index2016, sort_2016.value AS value2016 FROM top_2017 INNER JOIN sort_2016 ON top_2017.city = sort_2016.city AND top_2017.country = sort_2016.country")
      .collect()
  }
}
