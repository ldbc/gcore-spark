package compiler

import algebra.AlgebraRewriter
import algebra.trees.AlgebraContext
import parser.SpoofaxParser
import parser.trees.ParseContext

/** Defines the compilation pipeline of a G-CORE query. */
case class GcoreCompiler(compileContext: CompileContext) extends Compiler {

  val parser: ParseStage = SpoofaxParser(ParseContext(compileContext.graphDb))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(compileContext.graphDb))

  override def compile(query: String): Unit = (parser andThen rewriter)(query)
}
