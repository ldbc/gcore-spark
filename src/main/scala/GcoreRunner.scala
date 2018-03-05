import java.nio.file.Paths

import compiler.{CompileContext, Compiler, GcoreCompiler}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import spark.SparkGraphDb

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

    graphDb.registerGraph(
      graphDb.jsonSource,
      Paths.get("/export/scratch1/georgian/repos/gcore-interpreter/src/main/scala/spark/examples/dummy_graph.json"))

    println(graphDb.graph("dummy_graph"))

    val compiler: Compiler = GcoreCompiler(CompileContext(graphDb))
    compiler.compile(
      """
        | CONSTRUCT () MATCH (:Cat {onDiet = true})->(:Food) ON dummy_graph")
      """.stripMargin)
  }
}
