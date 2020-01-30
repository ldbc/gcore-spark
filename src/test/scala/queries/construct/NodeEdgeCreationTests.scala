package construct

import algebra.AlgebraRewriter
import algebra.expressions.Label
import algebra.trees.{AlgebraContext, AlgebraTreeNode}
import compiler.{CompileContext, ParseStage, RewriteStage, RunTargetCodeStage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import parser.SpoofaxParser
import parser.trees.ParseContext
import schema.{PathPropertyGraph, Table}
import spark.SparkCatalog
import spark.examples.SocialGraph
import spark.sql.SqlRunner

@RunWith(classOf[JUnitRunner])
class NodeEdgeCreationTests extends FunSuite{

  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("G-CORE Runner")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._
  val catalog: SparkCatalog = SparkCatalog(sparkSession)
  catalog.registerGraph(SocialGraph(sparkSession))
  catalog.setDefaultGraph("social_graph")
  val context = CompileContext(catalog, sparkSession)
  val parser: ParseStage = SpoofaxParser(ParseContext(context.catalog))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(context.catalog))
  val target: RunTargetCodeStage = SqlRunner(context)

  test("Create new edge with property assignment"){
    val query = "CONSTRUCT (n)-[e:Related {prop:='value'}]->(m) MATCH (n:Tag), (m:Place)"
    val expectedTag = Seq(
      ("106","Wagner")
    ).toDF("id", "name")
    val expectedPlace = Seq(
      ("105","Houston")
    ).toDF("id","name")
    val expectedEdge = Seq(
      ("106","105","e0","value")
    ).toDF("fromId", "toId", "id", "prop")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.except(expectedEdge).count == 0)
  }
}
