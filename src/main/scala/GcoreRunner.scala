import ir.{IrException, SpoofaxToIrRewriter}
import org.slf4j.LoggerFactory
import org.spoofax.interpreter.terms.IStrategoTerm._
import org.spoofax.interpreter.terms.{IStrategoAppl, IStrategoInt, IStrategoString, IStrategoTerm}
import utils.Gcore

/** Main entry point of the interpreter. */
object GcoreRunner extends App {
  val Query = "" +
    "CONSTRUCT () MATCH (n {n.foo = bar}) ON social_graph"
  val Logger = LoggerFactory.getLogger(this.getClass.getName)

  val gcore: Gcore = new Gcore()
  val ast: IStrategoTerm = gcore.parseQuery(Query)

  Logger.debug("{}", ast.toString())

  println("Pretty printer:")
  preOrder(ast)

  println("\n\nIR:")
  val rewriter: SpoofaxToIrRewriter = new SpoofaxToIrRewriter()
  rewriter.process(ast).print()

  @throws(classOf[IrException])
  def preOrder(term: IStrategoTerm): Unit = {

    @throws(classOf[IrException])
    def withIndent(term: IStrategoTerm, level: Int): Unit = {
      term.getTermType match {
        case APPL =>
          println(" " * level + term.asInstanceOf[IStrategoAppl].getConstructor.getName)
          term.getAllSubterms.foreach(subTerm => withIndent(subTerm, level + 2))
        case STRING =>
          println(" " * level + term.asInstanceOf[IStrategoString].stringValue())
        case INT =>
          println(" " * level + term.asInstanceOf[IStrategoInt].intValue())
        case IStrategoTerm.LIST =>
          var name: String = if (term.getSubtermCount == 0) "EmptyList" else "List"
          println(" " * level + name)
          term.getAllSubterms.foreach(subTerm => withIndent(subTerm, level + 2))
        case _ => throw new IrException("Don't understand term of type " + term.getTermType)
      }
    }

    withIndent(term, 0)
  }
}