import api.compiler.{Compiler, ParseStage, RewriteStage}
import com.google.inject.Inject

/** Defines the compilation pipeline of a G-CORE query. */
object GcoreCompiler extends Compiler {

  @Inject val parser: ParseStage = null
  @Inject val rewriter: RewriteStage = null

  override def compile(query: String): Unit = (parser andThen rewriter)(query)
}
