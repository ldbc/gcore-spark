import compiler.{CompileContext, Compiler, GcoreCompiler}
import org.apache.spark.sql.SparkSession
import schema.Catalog
import spark.SparkCatalog
import spark.examples.{DummyGraph, PathsGraph, PeopleGraph}

/** Main entry point of the interpreter. */
object GcoreRunner {

  def newRunner: GcoreRunner = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("G-CORE Runner")
      .master("local[*]")
      .getOrCreate()
    val catalog: SparkCatalog = SparkCatalog(sparkSession)
    val compiler: Compiler = GcoreCompiler(CompileContext(catalog, sparkSession))

    GcoreRunner(sparkSession, compiler, catalog)
  }

  def main(args: Array[String]): Unit = {
    val gcoreRunner: GcoreRunner = GcoreRunner.newRunner
    gcoreRunner.catalog.registerGraph(DummyGraph(gcoreRunner.sparkSession))
    gcoreRunner.catalog.registerGraph(PeopleGraph(gcoreRunner.sparkSession))
    gcoreRunner.catalog.registerGraph(PathsGraph(gcoreRunner.sparkSession))
    gcoreRunner.catalog.setDefaultGraph("paths_graph")

    gcoreRunner.compiler.compile(
      """
        | CONSTRUCT (v)
        | MATCH (v)-/p <:knows*>/->(w)
      """.stripMargin)
  }
}

case class GcoreRunner(sparkSession: SparkSession, compiler: Compiler, catalog: Catalog)
