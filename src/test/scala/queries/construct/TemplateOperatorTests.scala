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
class TemplateOperatorTests extends FunSuite {
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

  test("3.1.1 HAVING operator"){
    val query = "CONSTRUCT (n) HAVING n.lastName = 'Gold' MATCH (n:Person)"
    val expected = Seq(
      ("100")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("3.1.2 SET Operator on node"){
    val query = "CONSTRUCT (n) SET n.prop := 'value' MATCH (n:Person)"
    val expected = Seq(
      ("100", "value"),
      ("101", "value"),
      ("102", "value"),
      ("103", "value"),
      ("104", "value")
    ).toDF("id", "prop")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id", "prop").except(expected).count == 0)
  }

  test("3.1.3 SET Operator with variable"){
    val query = "CONSTRUCT (x:Employer) SET x.name := n.employer MATCH (n:Person)"
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

  test("3.1.4 SET operator on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) SET e.person := n.firstName MATCH (n)-[e:HasInterest]->(m)"
    val expectedPerson = Seq(
      ("104"), ("103")
    ).toDF("id")
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedEdge = Seq(
      ("500","Celine"),("501","Alice")
    ).toDF("id","person")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("HasInterest")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id","person").except(expectedEdge).count == 0)
  }

  test("3.1.5 REMOVE operator on node"){
    val query = "CONSTRUCT (n) REMOVE n.employer MATCH (n:Person)"
    val expected = Seq(
      ("100"),("101"),("102"),("103"),("104")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
    assert(!result.columns.contains("employer"))
  }

  test("3.1.6 REMOVE operator on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) REMOVE e.since MATCH (n)-[e:HasInterest]->(m)"
    val expectedPerson = Seq(
      ("104"), ("103")
    ).toDF("id")
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedEdge = Seq(
      ("500"),("501")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("HasInterest")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
    assert(!resultEdge.columns.contains("since"))
  }

  test("3.1.7 GROUP operator"){
    val query = "CONSTRUCT (x GROUP n.employer :Employer {name:=n.employer}) MATCH (n:Person)"
    val expected = Seq(
      ("[MIT][CWI]"),("Acme"),("HAL")
    ).toDF("name")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Employer")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("name").except(expected).count == 0)
  }
}
