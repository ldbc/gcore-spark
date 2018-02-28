package spark

import java.nio.file.{Path, Paths}

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/** A generic source that can load and create a [[SparkGraph]] based on a [[GraphJsonConfig]]. */
abstract class GraphSource(spark: SparkSession) {

  /** Loads a [[SparkGraph]] from a JSON config file at the given path. */
  def loadGraph(graphConfigPath: Path): SparkGraph =
    loadGraph(GraphSource parseJsonConfig graphConfigPath)

  /** Loads a [[SparkGraph]] from a [[GraphJsonConfig]] object. */
  def loadGraph(graphConfig: GraphJsonConfig): SparkGraph
}

object GraphSource {

  implicit val format: DefaultFormats.type = DefaultFormats

  /**
    * Creates a [[GraphJsonConfig]] object by deserializing the config JSON stored at the given
    * path.
    */
  def parseJsonConfig(configPath: Path): GraphJsonConfig = {
    val configs = Source.fromFile(configPath.toString).mkString
    val json = parse(configs)
    json.camelizeKeys.extract[GraphJsonConfig]
  }

  /** Available [[GraphSource]]s. */
  val json: JsonGraphSource.type = JsonGraphSource
  val parquet: ParquetGraphSource.type = ParquetGraphSource
}

/** Expected schema of a JSON config file of a [[SparkGraph]]. */
case class GraphJsonConfig(graphName: String,
                           graphRootDir: String,
                           vertexLabels: Seq[String],
                           edgeLabels: Seq[String],
                           pathLabels: Seq[String]) {

  val vertexFiles: Seq[Path] = vertexLabels.map(Paths.get(graphRootDir,  _))
  val edgeFiles: Seq[Path] = edgeLabels.map(Paths.get(graphRootDir,  _))
  val pathFiles: Seq[Path] = pathLabels.map(Paths.get(graphRootDir,  _))
}
