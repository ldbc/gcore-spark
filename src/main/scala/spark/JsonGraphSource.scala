package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Can import data stored in JSON format into a [[DataFrame]]. */
case class JsonGraphSource(spark: SparkSession) extends GraphSource(spark) {
  override val loadDataFn: String => DataFrame = spark.read.json
}
