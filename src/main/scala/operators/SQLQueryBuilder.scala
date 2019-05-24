package operators

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SQL Query Builder
  * @param spark
  * @param table
  */
class SQLQueryBuilder(val spark: SparkSession, val table: String) {
  def sql(dest: String, query: String): SQLQueryBuilder = {
    val finalQuery = query.replace("{TABLE_NAME}", this.table)
    this.spark.sql(finalQuery).createOrReplaceTempView(dest)
    new SQLQueryBuilder(this.spark, dest)
  }

  def collect(): DataFrame = {
    this.spark.sql("SELECT * FROM {TABLE_NAME}".replace("{TABLE_NAME}", this.table))
  }

  def cache(): SQLQueryBuilder = {
    this.sql(this.table, "CACHE TABLE {TABLE_NAME}".replace("{TABLE_NAME}", this.table))
  }
}
