package spark

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import schema.{GraphData, GraphSchema}

/**
  * Tests that a [[GraphSource]] successfully loads a [[SparkGraph]] from any available format and
  * correctly fills in the [[GraphData]] within the graph. Does not verify that the [[GraphSchema]]
  * is built correctly, as this tested is separately in [[SparkGraphTest]].
  */
class GraphSourceTest extends FunSuite
  with SparkSessionTestWrapper
  with TemporaryFolder
  with TestGraphWrapper
  with BeforeAndAfterAll {

  import spark.implicits._

  val peopleDf: DataFrame = peopleList.toDF
  val cityDf: DataFrame = cityList.toDF
  val bornInDf: DataFrame = bornInList.toDF
  val roadDf: DataFrame = roadList.toDF
  val travelRouteDf: DataFrame = travelRouteList.toDF

  override def afterAll(): Unit = {
    super.afterAll()
    deleteTestDir()
  }

  test("JsonGraphSource") {
    val rootDir = newDir("json")
    peopleDf.repartition(1).write.json(rootDir.getPath + "/person")
    cityDf.repartition(1).write.json(rootDir.getPath + "/city")
    bornInDf.repartition(1).write.json(rootDir.getPath + "/bornIn")
    roadDf.repartition(1).write.json(rootDir.getPath + "/road")
    travelRouteDf.repartition(1).write.json(rootDir.getPath + "/travelRoute")

    val json =
      s"""
         |{
         |"graph_name": "test_graph",
         |"graph_root_dir": "${rootDir.getPath}",
         |"vertex_labels": ["person", "city"],
         |"edge_labels": ["bornIn", "road"],
         |"path_labels": ["travelRoute"]
         |} """.stripMargin

    Files.write(Paths.get(rootDir.getPath, "config.json"), json.getBytes(UTF_8))
    val graphSource = GraphSource.json(spark)
    val graph = graphSource.loadGraph(Paths.get(testDir.getPath, "json", "config.json"))

    runTestOn(graph)
  }

  test("ParquetGraphSource") {
    val rootDir = newDir("parquet")
    peopleDf.repartition(1).write.parquet(rootDir.getPath + "/person")
    cityDf.repartition(1).write.parquet(rootDir.getPath + "/city")
    bornInDf.repartition(1).write.parquet(rootDir.getPath + "/bornIn")
    roadDf.repartition(1).write.parquet(rootDir.getPath + "/road")
    travelRouteDf.repartition(1).write.parquet(rootDir.getPath + "/travelRoute")

    val json =
      s"""
         |{
         |"graph_name": "test_graph",
         |"graph_root_dir": "${rootDir.getPath}",
         |"vertex_labels": ["person", "city"],
         |"edge_labels": ["bornIn", "road"],
         |"path_labels": ["travelRoute"]
         |} """.stripMargin

    Files.write(Paths.get(rootDir.getPath, "config.json"), json.getBytes(UTF_8))
    val graphSource = GraphSource.parquet(spark)
    val graph = graphSource.loadGraph(Paths.get(testDir.getPath, "parquet", "config.json"))

    runTestOn(graph)
  }

  private def runTestOn(graph: SparkGraph): Unit = {
    // It would be nicer to just call compareDfs here, but upon reading from file, Spark sometimes
    // changes the order in which the columns of the table have been written (also by Spark) in that
    // file, making intersect and except calls to fail. We impose a column order on the DataFrame by
    // selecting the columns as we wish.
    comparePerson(graph.tableMap("person").data)
    compareCity(graph.tableMap("city").data)
    compareBornIn(graph.tableMap("bornIn").data)
    compareRoad(graph.tableMap("road").data)
    compareTravelRoute(graph.tableMap("travelRoute").data)
  }

  private def comparePerson(actual: DataFrame): Unit = {
    compareDfs(actual = actual.select("id", "name", "age", "isAlive"),
      expected = peopleDf.select("id", "name", "age", "isAlive"))
  }

  private def compareCity(actual: DataFrame): Unit = {
    compareDfs(actual = actual.select("id", "name"),
      expected = cityDf.select("id", "name"))
  }

  private def compareBornIn(actual: DataFrame): Unit = {
    compareDfs(actual = actual.select("id", "toId", "fromId", "hasLeft"),
      expected = bornInDf.select("id", "toId", "fromId", "hasLeft"))
  }

  private def compareRoad(actual: DataFrame): Unit = {
    compareDfs(actual = actual.select("id", "fromId", "toId"),
      expected = roadDf.select("id", "fromId", "toId"))
  }

  private def compareTravelRoute(actual: DataFrame): Unit = {
    // TODO: Also compare on edges.
    //
    // When reading from Parquet format, the except call fails on the edges column with the error
    // that the two columns (expected.edges and actual.edges) are of different type. This is weird,
    // because upon inspection, they are both arrays of integers. What makes it even weirder is that
    // for the json format calling except with the edges column work just fine, but the schema shows
    // that the expected.edges column is an array of integers, whereas actual.edges is an array of
    // longs. What generates this issue?
    compareDfs(actual = actual.select("id"),
      expected = travelRouteDf.select("id"))
  }

  private def compareDfs(actual: DataFrame, expected: DataFrame): Unit = {
    assert(actual.except(expected).count() == 0)
    assert(expected.except(actual).count() == 0)
  }
}
