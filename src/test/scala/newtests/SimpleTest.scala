package newtests

import algebra.AlgebraRewriter
import algebra.trees.{AlgebraContext, AlgebraTreeNode}
import compiler.{CompileContext, ParseStage, RewriteStage, RunTargetCodeStage}
import org.apache.spark.sql.{DataFrame, SparkSession}


import collection.mutable.Stack
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import parser.SpoofaxParser
import parser.trees.ParseContext
import schema.PathPropertyGraph
import spark.SparkCatalog
import spark.examples.SocialGraph
import spark.sql.SqlRunner
import schema.Table

@RunWith(classOf[JUnitRunner])
class SimpleTest extends FunSuite{
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
  //val compiler: Compiler = GcoreCompiler(CompileContext(catalog, sparkSession))
  val parser: ParseStage = SpoofaxParser(ParseContext(context.catalog))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(context.catalog))
  val target: RunTargetCodeStage = SqlRunner(context)

  test("A simple test") {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    val oldSize = stack.size
    val result = stack.pop()
    assert(result === 2)
    assert(stack.size === oldSize - 1)
  }

  test("Testing a simple query") {
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
    val result: DataFrame = graph.vertexData.head.asInstanceOf[Table[DataFrame]].data
    assert(result.except(expected).count == 0, true)
  }
}
