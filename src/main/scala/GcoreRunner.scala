import api.compiler.{Compiler, ParseStage, RewriteStage}
import com.google.inject.{AbstractModule, Guice, Injector}
import ir.trees.AlgebraRewriter
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import parser.SpoofaxParser
import schema.GraphDb
import spark.SparkGraphDb

/** Main entry point of the interpreter. */
object GcoreRunner extends AbstractModule {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("G-CORE Runner")
      .master("local[*]")
      .getOrCreate()

    val injector: Injector = Guice.createInjector(GcoreRunner)
    injector.getInstance(classOf[Compiler]).compile("" +
      "CONSTRUCT () MATCH ()")
  }

  override def configure(): Unit = {
    bind(classOf[Compiler]).toInstance(GcoreCompiler)
    bind(classOf[ParseStage]).toInstance(SpoofaxParser)
    bind(classOf[RewriteStage]).toInstance(AlgebraRewriter)
    bind(classOf[GraphDb[_]]).toInstance(SparkGraphDb)
  }
}
