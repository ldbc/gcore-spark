import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import spark.examples.DummyGraph
import spark.{GraphSource, JsonGraphSource, SparkGraph}

/** Main entry point of the interpreter. */
object GcoreRunner {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("G-CORE Runner")
      .master("local[*]")
      .getOrCreate()

    manualGraph(spark)
  }

  def manualGraph(sparkSession: SparkSession): Unit = {
    val graph: SparkGraph = DummyGraph(sparkSession)
    logger.info(graph.toString)
    logger.info(graph.sparkSchemaString)
  }

  def jsonExample1(sparkSession: SparkSession): Unit = {
    val graph: SparkGraph =
      GraphSource.json(sparkSession).loadGraph(Paths.get("/path/to/dummy_graph.json"))
    logger.info(graph.toString)
    logger.info(graph.sparkSchemaString)
  }

  def jsonExample2(sparkSession: SparkSession): Unit = {
    val graph: SparkGraph =
      JsonGraphSource(sparkSession).loadGraph(Paths.get("/path/to/dummy_graph.json"))
    logger.info(graph.toString)
    logger.info(graph.sparkSchemaString)
  }
}