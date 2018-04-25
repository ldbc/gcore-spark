import compiler.{CompileContext, Compiler, GcoreCompiler}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import spark.SparkGraphDb
import spark.examples.{DummyGraph, PeopleGraph}

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
    graphDb.registerGraph(PeopleGraph(spark))
    graphDb.setDefaultGraph("people_graph")

    val compiler: Compiler = GcoreCompiler(CompileContext(graphDb, spark.newSession()))
    compiler.compile(
      """
        | CONSTRUCT (x GROUP p.employer)-(p {newProp := p.name})
        | MATCH (c:Company)<-[e]-(p:Person)
        | WHERE c.name = p.employer
      """.stripMargin)
  }
}
