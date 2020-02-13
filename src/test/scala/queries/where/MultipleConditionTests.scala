package where

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
class MultipleConditionTests extends FunSuite {
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

  test("2.2.1 AND Operator on Node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer='Acme' AND n.university='Yale'"
    val expected = Seq(
      ("Hoffman","104","Acme","Alice","Yale")
    ).toDF("lastName", "id","employer","firstName","university")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("lastName", "id","employer","firstName","university").except(expected).count == 0)
  }

  test("2.2.2 AND Operator on Edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:Knows]->(m:Person) WHERE e.nr_messages='3' AND e.fromId='102'"
    val expectedPerson = Seq(
      ("Hoffman","104","Acme","Alice","Yale"),
      ("Smith","102",null,"Peter","Stanford")
    ).toDF("lastName", "id","employer","firstName","university")
    val expectedEdge = Seq(
      ("205","102","104","3")
    ).toDF("id","fromId","toId","nr_messages")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("lastName", "id","employer","firstName","university").except(expectedPerson).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Knows")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id","fromId","toId","nr_messages").except(expectedEdge).count == 0)
  }

  test("2.2.3 OR Operator on Node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer='Acme' OR n.employer='HAL'"
    val expected = Seq(
      ("Doe","101","Acme","John","Oxford"),
      ("Mayer","103","HAL","Celine","Harvard"),
      ("Hoffman","104","Acme","Alice","Yale")
    ).toDF("lastName", "id","employer","firstName","university")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("lastName", "id","employer","firstName","university").except(expected).count == 0)
  }

  test("2.2.4 OR Operator on Edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:Knows]->(m:Person) WHERE e.nr_messages='3' OR e.nr_messages='2'"
    val expectedPerson = Seq(
      ("Hoffman","104","Acme","Alice","Yale"),
      ("Smith","102",null,"Peter","Stanford"),
      ("Gold","100","[MIT][CWI]","Frank","Harvard")
    ).toDF("lastName", "id","employer","firstName","university")
    val expectedEdge = Seq(
      ("204","100","102","2"),
      ("205","102","104","3"),
      ("210","102","100","2"),
      ("211","104","102","3")
    ).toDF("id","fromId","toId","nr_messages")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("lastName", "id","employer","firstName","university").except(expectedPerson).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Knows")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id","fromId","toId","nr_messages").except(expectedEdge).count == 0)
  }

  //AND-OR with IS NULL and IS NOT NULL
}
