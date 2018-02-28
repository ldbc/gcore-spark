package spark

import org.apache.spark.sql.SparkSession

/** Provides the [[SparkSession]] for a Spark test. */
trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("G-CORE interpreter test suite")
      .getOrCreate()
  }
}
