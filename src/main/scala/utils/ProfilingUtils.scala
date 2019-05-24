package utils

import org.apache.spark.sql.DataFrame

/**
  * Profiling Utils
  */
object ProfilingUtils {
  def timeDataFrame(df: DataFrame, id: String): DataFrame = {
    val start = System.nanoTime()
    df.first()
    val end = System.nanoTime()

    println(s"$id - ${(end - start) / 1e09} sec")
    df
  }
}
