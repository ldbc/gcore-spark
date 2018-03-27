package compiler

import algebra.AlgebraRewriter
import algebra.trees.AlgebraContext
import parser.SpoofaxParser
import parser.trees.ParseContext
import planner.QueryPlanner
import planner.trees.PlannerContext
import spark.sql.SparkSqlRunner

/** Defines the compilation pipeline of a G-CORE query. */
case class GcoreCompiler(context: CompileContext) extends Compiler {

  val parser: ParseStage = SpoofaxParser(ParseContext(context.graphDb))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(context.graphDb))
  val planner: PlanningStage = QueryPlanner(PlannerContext(context.graphDb))
  val target: RunTargetCodeStage = SparkSqlRunner(context.sparkSession)

  override def compile(query: String): Unit =
    (parser andThen rewriter andThen planner andThen target) (query)
}
