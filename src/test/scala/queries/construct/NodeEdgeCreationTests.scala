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
import spark.examples.SocialTestGraph
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
  catalog.registerGraph(SocialTestGraph(sparkSession))
  catalog.setDefaultGraph("social_test_graph")
  val context = CompileContext(catalog, sparkSession)
  val parser: ParseStage = SpoofaxParser(ParseContext(context.catalog))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(context.catalog))
  val target: RunTargetCodeStage = SqlRunner(context)

  test("3.3.1 Create new edge"){
    val query = "CONSTRUCT (n)-[e:Related]->(m) MATCH (n:Tag), (m:Place)"
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105"), ("115")
    ).toDF("id")
    val expectedEdge = Seq(
      ("106","105"), ("106","115")
    ).toDF("fromId", "toId")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("fromId", "toId").except(expectedEdge).count == 0)
  }

  test("3.3.2 Create new edge with property assignment"){
    val query = "CONSTRUCT (n)-[e:Related {prop:='value'}]->(m) MATCH (n:Tag), (m:Place)"
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105"), ("115")
    ).toDF("id")
    val expectedEdge = Seq(
      ("106","105","value"), ("106","115","value")
    ).toDF("fromId", "toId", "prop")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("fromId","toId", "prop").except(expectedEdge).count == 0)
  }

  test("3.3.3 Create new edge with property from node"){
    val query = "CONSTRUCT (n)-[e:Related {tag:=n.name, place:=m.name}]->(m) MATCH (n:Tag), (m:Place)"
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105"), ("115")
    ).toDF("id")
    val expectedEdge = Seq(
      ("106","105","Wagner","Houston"), ("106","115","Wagner","New York")
    ).toDF("fromId", "toId", "tag", "place")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    resultEdge.show
    expectedEdge.show
    assert(resultEdge.select("fromId", "toId", "tag", "place").except(expectedEdge).count == 0)
  }

  test("3.3.4 Create new node"){
    val query = "CONSTRUCT (x:Employer) MATCH (n:Person)"
    val expected = Seq(
      ("x0"),
      ("x1"),
      ("x2"),
      ("x3"),
      ("x4")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Employer")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("3.3.5 Create new node with property assignment"){
    val query = "CONSTRUCT (x:Employer {prop:='value'}) MATCH (n:Person)"
    val expected = Seq(
      ("x0", "value"),
      ("x1", "value"),
      ("x2", "value"),
      ("x3", "value"),
      ("x4", "value")
    ).toDF("id", "prop")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Employer")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id", "prop").except(expected).count == 0)
  }

  test("3.3.6 Create new node with property from node"){
    val query = "CONSTRUCT (x:Employer {name:=n.employer}) MATCH (n:Person)"
    val expected = Seq(
      ("Acme"),
      ("HAL"),
      ("Acme"),
      (null),
      ("[MIT][CWI]")
    ).toDF("name")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Employer")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("name").except(expected).count == 0)
  }
}
