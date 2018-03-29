package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

case class ParquetGraphSource(spark: SparkSession) extends GraphSource(spark) {
  override val loadDataFn: String => DataFrame = spark.read.parquet
}
