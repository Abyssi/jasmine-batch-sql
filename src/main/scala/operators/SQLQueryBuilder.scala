package operators

import org.apache.spark.sql.{DataFrame, SparkSession}

class SQLQueryBuilder(val spark: SparkSession, val table: String) {
  //var query = ""
  def sql(dest: String, query: String): SQLQueryBuilder = {
    val finalQuery = query.replace("{TABLE_NAME}", this.table)
    this.spark.sql(finalQuery).createOrReplaceTempView(dest)
    new SQLQueryBuilder(this.spark, dest)
    /*
    val builder = new SQLQueryBuilder(this.spark, dest)
    builder.query = query.replace("{TABLE_NAME}", s"(${this.query})")
    builder
    */
  }

  def collect(): DataFrame = {
    this.spark.sql("SELECT * FROM {TABLE_NAME}".replace("{TABLE_NAME}", this.table))
    //this.spark.sql(this.query)
  }

  def cache(): SQLQueryBuilder = {
    this.sql(this.table, "CACHE TABLE {TABLE_NAME}".replace("{TABLE_NAME}", this.table))
  }
}
