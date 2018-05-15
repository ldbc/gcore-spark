package spark

import java.nio.file.{Path, Paths}

import algebra.expressions.Label
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import schema.EntitySchema.LabelRestrictionMap
import schema.{SchemaMap, Table}

import scala.io.Source

/** A generic source that can load and create a [[SparkGraph]] based on a [[GraphJsonConfig]]. */
abstract class GraphSource(spark: SparkSession) {

  val loadDataFn: String => DataFrame

  /** Loads a [[SparkGraph]] from a JSON config file at the given path. */
  def loadGraph(graphConfigPath: Path): SparkGraph =
    loadGraph(GraphSource parseJsonConfig graphConfigPath)

  /** Loads a [[SparkGraph]] from a [[GraphJsonConfig]] object. */
  def loadGraph(graphConfig: GraphJsonConfig): SparkGraph =
    new SparkGraph {
      override def graphName: String = graphConfig.graphName

      override def pathData: Seq[Table[DataFrame]] = loadData(graphConfig.pathFiles)

      override def vertexData: Seq[Table[DataFrame]] = loadData(graphConfig.vertexFiles)

      override def edgeData: Seq[Table[DataFrame]] = loadData(graphConfig.edgeFiles)

      override def edgeRestrictions: LabelRestrictionMap =
        buildRestrictions(graphConfig.edgeRestrictions)

      override def storedPathRestrictions: LabelRestrictionMap =
        buildRestrictions(graphConfig.pathRestrictions)
  }

  private def loadData(dataFiles: Seq[Path]): Seq[Table[DataFrame]] =
    dataFiles.map(
      filePath =>
        Table(
          name = Label(filePath.getFileName.toString),
          data = loadDataFn(filePath.toString)))

  private def buildRestrictions(restrictions: Seq[ConnectionRestriction]): LabelRestrictionMap = {
    restrictions.foldLeft(SchemaMap.empty[Label, (Label, Label)]) {
      (aggSchemaMap, conRestr) => {
        aggSchemaMap union SchemaMap(Map(
          Label(conRestr.connLabel) ->
            (Label(conRestr.sourceLabel), Label(conRestr.destinationLabel))
        ))
      }
    }
  }
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
                           pathLabels: Seq[String],
                           edgeRestrictions: Seq[ConnectionRestriction],
                           pathRestrictions: Seq[ConnectionRestriction]) {

  validateSchema()

  val vertexFiles: Seq[Path] = vertexLabels.map(Paths.get(graphRootDir, _))
  val edgeFiles: Seq[Path] = edgeLabels.map(Paths.get(graphRootDir, _))
  val pathFiles: Seq[Path] = pathLabels.map(Paths.get(graphRootDir, _))

  def validateSchema(): Unit = {
    assert(vertexLabels.nonEmpty, invalidSchemaMessage("vertex_labels"))

    if (edgeLabels.nonEmpty)
      assert(edgeRestrictions.nonEmpty, invalidSchemaMessage("edge_restrictions"))

    if (pathLabels.nonEmpty)
      assert(pathRestrictions.nonEmpty, invalidSchemaMessage("path_restrictions"))
  }

  def invalidSchemaMessage(emptyField: String): String = {
    s"The provided configuration was invalid. The field $emptyField must be present in the json."
  }
}

case class ConnectionRestriction(connLabel: String,
                                 sourceLabel: String,
                                 destinationLabel: String)
