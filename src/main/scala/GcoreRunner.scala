import org.slf4j.LoggerFactory
import org.spoofax.interpreter.terms.IStrategoTerm
import utils.Gcore

object GcoreRunner extends App {
  val Query = "gcore-queries/query.gcore"
  val Logger = LoggerFactory.getLogger(this.getClass.getName)

  val gcore: Gcore = new Gcore()
  val ast: IStrategoTerm = gcore.parseQuery(Query)

  Logger.debug("{}", ast.toString())
}