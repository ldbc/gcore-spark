package `match`

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
class BasicMatchTests extends FunSuite{

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

  test("Node match with label") {
    val query = "CONSTRUCT (n) MATCH (n:Person)"
    val expected = Seq(
      ("Doe","101","Acme","John","Oxford"),
      ("Mayer","103","HAL","Celine","Harvard"),
      ("Hoffman","104","Acme","Alice","Yale"),
      ("Smith","102",null,"Peter","Stanford"),
      ("Gold","100","[MIT][CWI]","Frank","Harvard")
    ).toDF("lastName", "id","employer","firstName","university")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.except(expected).count == 0)
  }

  test("Edge match with only edge label"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n)-[e:IsLocatedIn]->(m)"
    val expectedPerson = Seq(
      ("Doe","101","Acme","John","Oxford"),
      ("Mayer","103","HAL","Celine","Harvard"),
      ("Hoffman","104","Acme","Alice","Yale")
    ).toDF("lastName", "id","employer","firstName","university")
    val expectedPlace = Seq(
      ("105","Houston")
    ).toDF("id","name")
    val expectedEdge = Seq(
      ("400", "101", "105"),
      ("402", "104", "105"),
      ("401", "103", "105")
    ).toDF("id", "fromId", "toId")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.except(expectedPerson).count == 0)

    //Place also has a timestamp field that is filled at execution time
    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.except(expectedEdge).count == 0)
  }

  test("Edge match with node and edge labels"){
    val query = "CONSTRUCT (n) MATCH (n:Person)-[:IsLocatedIn]->(m:Place)"
    val expected = Seq(
      ("Doe","101","Acme","John","Oxford"),
      ("Mayer","103","HAL","Celine","Harvard"),
      ("Hoffman","104","Acme","Alice","Yale")
    ).toDF("lastName", "id","employer","firstName","university")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.except(expected).count == 0)
  }

  test("Edge match with only node labels"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e]->(m:Place)"
    val expectedPerson = Seq(
      ("Doe","101","Acme","John","Oxford"),
      ("Mayer","103","HAL","Celine","Harvard"),
      ("Hoffman","104","Acme","Alice","Yale")
    ).toDF("lastName", "id","employer","firstName","university")
    val expectedPlace = Seq(
      ("105","Houston")
    ).toDF("id","name")
    val expectedEdge = Seq(
      ("400", "101", "105"),
      ("402", "104", "105"),
      ("401", "103", "105")
    ).toDF("id", "fromId", "toId")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.except(expectedPerson).count == 0)

    //Place also has a timestamp field that is filled at execution time
    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.except(expectedEdge).count == 0)
  }

  test("Multiple match patterns"){
    val query = "CONSTRUCT (n) MATCH (n:Person), (m:Tag)"
    val expected = Seq(
      ("Doe","101","Acme","John","Oxford"),
      ("Mayer","103","HAL","Celine","Harvard"),
      ("Hoffman","104","Acme","Alice","Yale"),
      ("Smith","102",null,"Peter","Stanford"),
      ("Gold","100","[MIT][CWI]","Frank","Harvard")
    ).toDF("lastName", "id","employer","firstName","university")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.except(expected).count == 0)
  }
}
