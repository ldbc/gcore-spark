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

  test("3.3.1 Create new edge"){
    val query = "CONSTRUCT (n)-[e:Related]->(m) MATCH (n:Tag), (m:Place)"
    val expectedTag = Seq(
      ("106","Wagner")
    ).toDF("id", "name")
    val expectedPlace = Seq(
      ("105","Houston")
    ).toDF("id","name")
    val expectedEdge = Seq(
      ("106","105","e0")
    ).toDF("fromId", "toId", "id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id", "name").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("fromId", "toId", "id").except(expectedEdge).count == 0)
  }

  test("3.3.2 Create new edge with property assignment"){
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
    assert(resultTag.select("id", "name").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("fromId","toId", "id", "prop").except(expectedEdge).count == 0)
  }

  test("3.3.3 Create new edge with property from node"){
    val query = "CONSTRUCT (n)-[e:Related {tag:=n.name, place:=m.name}]->(m) MATCH (n:Tag), (m:Place)"
    val expectedTag = Seq(
      ("106","Wagner")
    ).toDF("id", "name")
    val expectedPlace = Seq(
      ("105","Houston")
    ).toDF("id","name")
    val expectedEdge = Seq(
      ("106","105","e0","Wagner","Houston")
    ).toDF("fromId", "toId", "id", "tag", "place")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id", "name").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Related")).asInstanceOf[Table[DataFrame]].data
    resultEdge.show
    expectedEdge.show
    assert(resultEdge.select("fromId", "toId", "id", "tag", "place").except(expectedEdge).count == 0)
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
    assert(result.except(expected).count == 0)
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
