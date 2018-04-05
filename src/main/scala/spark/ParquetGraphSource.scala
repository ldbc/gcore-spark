package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Can import data stored in Parquet format into a [[DataFrame]]. */
case class ParquetGraphSource(spark: SparkSession) extends GraphSource(spark) {
  override val loadDataFn: String => DataFrame = spark.read.parquet
}
