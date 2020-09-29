/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (2018)
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark

import java.io.File
import java.nio.file.{Path, Paths}

import algebra.expressions.Label
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

      override var graphName: String = graphConfig.graphName

      override def pathData: Seq[Table[DataFrame]] = loadData(graphConfig.pathFiles)

      override def vertexData: Seq[Table[DataFrame]] = loadData(graphConfig.vertexFiles)

      override def edgeData: Seq[Table[DataFrame]] = loadData(graphConfig.edgeFiles)

      override def edgeRestrictions: LabelRestrictionMap =
        buildRestrictions(graphConfig.edgeRestrictions)

      override def storedPathRestrictions: LabelRestrictionMap =
        buildRestrictions(graphConfig.pathRestrictions)

    }

  /** Loads a [[SparkGraph]] from a single JSON file containing all data. */
  def loadGraph(graphFullFile: FSDataInputStream): SparkGraph ={
    val json = GraphSource parseFullGraphJson graphFullFile
    var vertexes = Seq[Any]()
    json.vertexes.foreach(m => m.foreach{
      case (vertexLabel:String, vertexFile:String) =>
        /*val rows = vertexData.map(m => Row(m.values.toSeq :_*))
        val header = vertexData.head.keys.toList
        val schema = StructType(header.map(fieldName => StructField(fieldName, StringType, true)))
        val dataRDD = spark.sparkContext.parallelize(rows)
        val df = spark.createDataFrame(dataRDD, schema)*/
        val df = spark.read.json(vertexFile)
        val table = (Table(Label(vertexLabel), df.cache()))
        vertexes = vertexes :+ table
    })
    var edges = Seq[Any]()
    json.edges.foreach(m => m.foreach{
      case (edgeLabel:String, edgeFile:String) =>
        /*val rows = edgeData.map(m => Row(m.values.toSeq :_*))
        val header = edgeData.head.keys.toList
        val schema = StructType(header.map(fieldName => StructField(fieldName, StringType, true)))
        val dataRDD = spark.sparkContext.parallelize(rows)
        val df = spark.createDataFrame(dataRDD, schema)*/
        val df = spark.read.json(edgeFile)
        val table = (Table(Label(edgeLabel), df.cache()))
        edges = edges :+ table
    })
    new SparkGraph{
      override var graphName: String = json.graphName

      override def vertexData: Seq[Table[DataFrame]] = vertexes.asInstanceOf[Seq[Table[DataFrame]]]

      override def edgeData: Seq[Table[DataFrame]] = edges.asInstanceOf[Seq[Table[DataFrame]]]

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def edgeRestrictions: LabelRestrictionMap = buildRestrictions(json.edgeRestrictions)

      override def storedPathRestrictions: LabelRestrictionMap = buildRestrictions(json.pathRestrictions)
    }
  }

  private def loadData(dataFiles: Seq[Path]): Seq[Table[DataFrame]] =
    dataFiles.map(
      filePath =>
        Table(
          name = Label(filePath.getFileName.toString),
          data = spark.sqlContext.read.json(filePath.toString).cache()))

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

  /**
   * Creates a [[SparkGraph]] from a JSON file fully containing it
   */
  def parseFullGraphJson(file: FSDataInputStream): FullGraphJson = {
    parse(file)
      .extract[FullGraphJson]
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

/** Expected schema of a JSON full graph file of a [[SparkGraph]]. */
case class FullGraphJson(graphName: String,
                         edgeRestrictions: Seq[ConnectionRestriction],
                         pathRestrictions: Seq[ConnectionRestriction],
                         vertexes: List[Map[String, Any]],
                         edges: List[Map[String, Any]]){
  implicit val format: DefaultFormats.type = DefaultFormats
  validateSchema()

  def validateSchema(): Unit ={
    assert(vertexes != null, "The provided configuration was invalid. The field vertexes must be present in the json.")
  }
}

case class ConnectionRestriction(connLabel: String,
                                 sourceLabel: String,
                                 destinationLabel: String)
