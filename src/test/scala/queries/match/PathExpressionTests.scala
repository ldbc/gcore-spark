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
import spark.examples.SocialTestGraph
import spark.sql.SqlRunner

@RunWith(classOf[JUnitRunner])
class PathExpressionTests extends FunSuite {
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

  /*
  test("1.3.1 Single label path"){
    val query = "CONSTRUCT (n)-[e:Reach]->(m) MATCH (n)-/<:IsLocatedIn>/->(m)"
    val expected = Seq(
      ("101","105"),
      ("103","105"),
      ("104","105")
    ).toDF("fromId","toId")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId").except(expected).count == 0)
  }


test("1.3.2 Single negated label path"){
  val query = "CONSTRUCT (n)-[e:Reach]->(m) MATCH (n)-/<:Knows!>/->(m:Message)"
  val expected = Seq(
    ("109", "107"),
    ("107", "110"),
    ("110", "108"),
    ("113", "112"),
    ("112", "111"),
    ("100", "112"),
    ("102", "109"),
    ("102", "110"),
    ("102", "111"),
    ("102", "113"),
    ("104", "107"),
    ("104", "108")
  ).toDF("fromId","toId")

  val rewrited: AlgebraTreeNode = rewriter(parser(query))
  val graph: PathPropertyGraph = target(rewrited)

  val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
  assert(result.select("fromId","toId").except(expected).count == 0)
}


  test("1.3.3 Single wildcard path"){
    val query = "CONSTRUCT (n)-[e:Reach]->(m) MATCH (n)-/<_>/->(m:Message)"
    val expected = Seq(
      ("109", "107"),
      ("107", "110"),
      ("110", "108"),
      ("113", "112"),
      ("112", "111"),
      ("100", "112"),
      ("102", "109"),
      ("102", "110"),
      ("102", "111"),
      ("102", "113"),
      ("104", "107"),
      ("104", "108")
    ).toDF("fromId","toId")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId").except(expected).count == 0)
  }

  test("1.3.4 Alternation of single labels"){
    val query = "CONSTRUCT (n)-[e:Reach]->(m) MATCH (n)-/<:ReplyOf|:IsLocatedIn>/->(m)"
    val expected = Seq(
      ("109", "107"),
      ("107", "110"),
      ("110", "108"),
      ("113", "112"),
      ("112", "111"),
      ("101", "105"),
      ("103", "105"),
      ("104", "105")
    ).toDF("fromId", "toId")


    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId").except(expected).count == 0)
  }
  */

  test("1.3.5 Concatenation of single labels"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:HasCreator :ReplyOf>/->(m)"
    val expectedPerson = Seq(
      ("100"), ("102"), ("104")
    ).toDF("id")
    val expectedMessage = Seq(
      ("108"),("111"),("112"),("107"),("113"),("109"),("110")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultMessage: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(resultMessage.select("id").except(expectedMessage).count == 0)
  }

  test("1.3.6 Plus Operator"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:ReplyOf+>/->(m)"
    val expectedMessage = Seq(
      ("108"),("111"),("112"),("107"),("113"),("109"),("110")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultMessage: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(resultMessage.select("id").except(expectedMessage).count == 0)
  }

  test("1.3.7 Star Operator"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:ReplyOf*>/->(m)"
    val expectedMessage = Seq(
      ("108"),("111"),("112"),("107"),("113"),("109"),("110")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultMessage: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(resultMessage.select("id").except(expectedMessage).count == 0)
  }

  test("1.3.8 Optional Operator"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:IsLocatedIn?>/->(m)"
    val expectedPerson = Seq(
      ("101"),
      ("103"),
      ("104")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)
  }

  test("1.3.9 Plus and concatenation"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:HasCreator :ReplyOf+>/->(m)"
    val expectedPerson = Seq(
      ("100"), ("102"), ("104")
    ).toDF("id")
    val expectedMessage = Seq(
      ("108"),("111"),("112"),("107"),("113"),("109"),("110")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultMessage: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(resultMessage.select("id").except(expectedMessage).count == 0)
  }

  test("1.3.10 Star and concatenation"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:HasCreator :ReplyOf*>/->(m)"
    val expectedPerson = Seq(
      ("100"), ("102"), ("104")
    ).toDF("id")
    val expectedMessage = Seq(
      ("108"),("111"),("112"),("107"),("113"),("109"),("110")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultMessage: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(resultMessage.select("id").except(expectedMessage).count == 0)
  }

  test("1.3.11 Optional and concatenation"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-/<:HasCreator :ReplyOf?>/->(m)"
    val expectedPerson = Seq(
      ("100"), ("102"), ("104")
    ).toDF("id")
    val expectedMessage = Seq(
      ("108"),("111"),("112"),("107"),("113"),("109"),("110")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultMessage: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(resultMessage.select("id").except(expectedMessage).count == 0)
  }

  //test("1.3.12 Single reverse label")
}
