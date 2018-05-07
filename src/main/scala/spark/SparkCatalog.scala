package spark

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.{Catalog, PathPropertyGraph}

/** A [[Catalog]] for [[PathPropertyGraph]]s backed by [[DataFrame]]s. */
case class SparkCatalog(sparkSession: SparkSession) extends Catalog {

  override type StorageType = DataFrame

  val jsonSource: JsonGraphSource = GraphSource.json(sparkSession)
  val parquetSource: ParquetGraphSource = GraphSource.parquet(sparkSession)

  /**
    * Register a graph coming from a [[GraphSource]] and configured through the config file stored
    * under the given path.
    */
  // TODO: Cache already registered graphs.
  def registerGraph(graphSource: GraphSource, configPath: Path): Unit = {
    val graph = graphSource.loadGraph(configPath)
    super.registerGraph(graph)
  }
}
