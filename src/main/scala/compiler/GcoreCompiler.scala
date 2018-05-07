package compiler

import algebra.AlgebraRewriter
import algebra.trees.AlgebraContext
import parser.SpoofaxParser
import parser.trees.ParseContext
import spark.sql.SqlRunner

/** Defines the compilation pipeline of a G-CORE query. */
case class GcoreCompiler(context: CompileContext) extends Compiler {

  val parser: ParseStage = SpoofaxParser(ParseContext(context.catalog))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(context.catalog))
  val target: RunTargetCodeStage = SqlRunner(context)

  override def compile(query: String): Unit =
    (parser andThen rewriter andThen target) (query)
}
