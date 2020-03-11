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
class PathConstructTests extends FunSuite {
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

  test("3.4.1 Single label path"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:IsLocatedIn>/->(m)"
    val expected = Seq(
      ("101","105", Seq(400)),
      ("104","105", Seq(402)),
      ("103","105", Seq(401))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.2 Single negated label"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:Knows!>/->(m:Message)"
    val expected = Seq(
      ("109", "107", Seq(300)),
      ("107", "110", Seq(301)),
      ("110", "108", Seq(302)),
      ("113", "112", Seq(303)),
      ("112", "111", Seq(304)),
      ("100", "112", Seq(600)),
      ("102", "109", Seq(601)),
      ("102", "110", Seq(602)),
      ("102", "111", Seq(603)),
      ("102", "113", Seq(604)),
      ("104", "107", Seq(605)),
      ("104", "108", Seq(606))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.3 Single wildcard"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:Knows!>/->(m:Message)"
    val expected = Seq(
      ("109", "107", Seq(300)),
      ("107", "110", Seq(301)),
      ("110", "108", Seq(302)),
      ("113", "112", Seq(303)),
      ("112", "111", Seq(304)),
      ("100", "112", Seq(600)),
      ("102", "109", Seq(601)),
      ("102", "110", Seq(602)),
      ("102", "111", Seq(603)),
      ("102", "113", Seq(604)),
      ("104", "107", Seq(605)),
      ("104", "108", Seq(606))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.4 Single reverse label"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:ReplyOf^>/->(m)"
    val expected = Seq(
      ("107", "109", Seq(300)),
      ("110", "107", Seq(301)),
      ("108", "110", Seq(302)),
      ("112", "113", Seq(303)),
      ("111", "112", Seq(304))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.5 Concatenation of single labels"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:HasCreator :ReplyOf>/->(m)"
    val expected = Seq(
      ("100", "111", Seq(600,304)),
      ("102", "107", Seq(601,300)),
      ("102", "108", Seq(602,302)),
      ("102", "112", Seq(604,303)),
      ("104", "110", Seq(605,301))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.6 Alternation of single labels"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:ReplyOf|:IsLocatedIn>/->(m)"
    val expected = Seq(
      ("109", "107", Seq(300)),
      ("107", "110", Seq(301)),
      ("110", "108", Seq(302)),
      ("113", "112", Seq(303)),
      ("112", "111", Seq(304)),
      ("101", "105", Seq(400)),
      ("103", "105", Seq(401)),
      ("104", "105", Seq(402))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.7 Plus operator"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:ReplyOf+>/->(m)"
    val expected = Seq(
      ("109", "107", Seq(300)),
      ("107", "110", Seq(301)),
      ("110", "108", Seq(302)),
      ("113", "112", Seq(303)),
      ("112", "111", Seq(304)),
      ("109", "110", Seq(300,301)),
      ("107", "108", Seq(301,302)),
      ("113", "111", Seq(303,304)),
      ("109", "108", Seq(300,301,302))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    result.show()
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.8 Star operator"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:ReplyOf*>/->(m)"
    val expected = Seq(
      ("109", "107", Seq(300)),
      ("107", "110", Seq(301)),
      ("110", "108", Seq(302)),
      ("113", "112", Seq(303)),
      ("112", "111", Seq(304)),
      ("109", "110", Seq(300,301)),
      ("107", "108", Seq(301,302)),
      ("113", "111", Seq(303,304)),
      ("109", "108", Seq(300,301,302))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.9 Optional operator"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:IsLocatedIn?>/->(m)"
    val expected = Seq(
      ("101","105", Seq(400)),
      ("104","105", Seq(402)),
      ("103","105", Seq(401))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.10 Concatenation with plus operator"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:HasCreator :ReplyOf+>/->(m)"
    val expected = Seq(
      ("102", "107", Seq(601,300)),
      ("104", "110", Seq(605,301)),
      ("102", "108", Seq(602,302)),
      ("102", "112", Seq(604,303)),
      ("100", "111", Seq(600,304)),
      ("102", "110", Seq(601,300,301)),
      ("104", "108", Seq(605,301,302)),
      ("102", "111", Seq(604,303,304)),
      ("102", "108", Seq(601,300,301,302))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.11 Concatenation with star operator"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:HasCreator :ReplyOf*>/->(m)"
    val expected = Seq(
      ("100", "112", Seq(600)),
      ("102", "109", Seq(601)),
      ("102", "110", Seq(602)),
      ("102", "111", Seq(603)),
      ("102", "113", Seq(604)),
      ("104", "107", Seq(605)),
      ("104", "108", Seq(606)),
      ("102", "107", Seq(601,300)),
      ("104", "110", Seq(605,301)),
      ("102", "108", Seq(602,302)),
      ("102", "112", Seq(604,303)),
      ("100", "111", Seq(600,304)),
      ("102", "110", Seq(601,300,301)),
      ("104", "108", Seq(605,301,302)),
      ("102", "111", Seq(604,303,304)),
      ("102", "108", Seq(601,300,301,302))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }

  test("3.4.12 Concatenation with optional operator"){
    val query = "CONSTRUCT (n)-/@p:Reach/->(m) MATCH (n)-/p<:HasCreator :ReplyOf?>/->(m)"
    val expected = Seq(
      ("100", "112", Seq(600)),
      ("102", "109", Seq(601)),
      ("102", "110", Seq(602)),
      ("102", "111", Seq(603)),
      ("102", "113", Seq(604)),
      ("104", "107", Seq(605)),
      ("104", "108", Seq(606)),
      ("102", "107", Seq(601,300)),
      ("104", "110", Seq(605,301)),
      ("102", "108", Seq(602,302)),
      ("102", "112", Seq(604,303)),
      ("100", "111", Seq(600,304))
    ).toDF("fromId","toId","edges")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val result: DataFrame = graph.tableMap(Label("Reach")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("fromId","toId", "edges").except(expected).count == 0)
  }
}
