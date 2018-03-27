package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Provides the [[SparkSession]] for a Spark test. */
trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("G-CORE interpreter test suite")
      .getOrCreate()
  }

  def compareDfs(actual: DataFrame, expected: DataFrame): Unit = {
    assert(actual.except(expected).count() == 0)
    assert(expected.except(actual).count() == 0)
  }
}
