import compiler.{CompileContext, Compiler, GcoreCompiler}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import spark.SparkGraphDb
import spark.examples.DummyGraph

/** Main entry point of the interpreter. */
object GcoreRunner {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("G-CORE Runner")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val graphDb: SparkGraphDb = SparkGraphDb(spark)

    graphDb.registerGraph(DummyGraph(spark))
    graphDb.setDefaultGraph("dummy_graph")

    println(graphDb.graph("dummy_graph"))

    val compiler: Compiler = GcoreCompiler(CompileContext(graphDb))
    compiler.compile(
      """
        | CONSTRUCT () MATCH (c:Cat)->(f:Food), (f: Food)<-(c:Cat)
      """.stripMargin)
//    compiler.compile(
//      """
//        | CONSTRUCT () MATCH (c:Cat), (f: Food)
//      """.stripMargin)
  }
}
