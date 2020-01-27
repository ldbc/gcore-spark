/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import algebra.expressions.Label
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import schema.{GraphData, GraphSchema}
/**
  * Test must be tested manually first and need closer inspection
  */

/**
  * Tests that a [[GraphSource]] successfully loads a [[SparkGraph]] from any available format and
  * correctly fills in the [[GraphData]] within the graph. Does not verify that the [[GraphSchema]]
  * is inferred correctly, as this tested is separately in [[SparkGraphTest]].
  */
//@RunWith(classOf[JUnitRunner])
class GraphSourceTest extends FunSuite
  with SparkSessionTestWrapper
  with TemporaryFolder
  with SimpleTestGraph
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

  test("A GraphSource infers the edge and path restrictions correctly") {
    val rootDir = newDir("empty_dfs")
    val json = createTestConfig(rootDir)
    Files.write(Paths.get(rootDir.getPath, "config.json"), json.getBytes(UTF_8))

    val graphSource = new GraphSource(spark) {
      override val loadDataFn: String => DataFrame = _ => spark.emptyDataFrame
    }
    val graph = graphSource.loadGraph(Paths.get(testDir.getPath, "empty_dfs", "config.json"))

    val edgeRestrictions: Map[Label, (Label, Label)] = graph.edgeRestrictions.map
    val pathRestrictions: Map[Label, (Label, Label)] = graph.storedPathRestrictions.map

    assert(edgeRestrictions.size == 2)
    assert(pathRestrictions.size == 1)

    assert(edgeRestrictions.toSet ==
      Set(
        (Label("BornIn"), (Label("Person"), Label("City"))),
        (Label("Road"), (Label("City"), Label("City"))))
    )

    assert(pathRestrictions.toSet ==
      Set((Label("TravelRoute"), (Label("City"), Label("City"))))
    )
  }

  test("A JsonGraphSource infers the graph data correctly") {
    val rootDir = newDir("json")
    peopleDf.repartition(1).write.json(rootDir.getPath + "/person")
    cityDf.repartition(1).write.json(rootDir.getPath + "/city")
    bornInDf.repartition(1).write.json(rootDir.getPath + "/bornIn")
    roadDf.repartition(1).write.json(rootDir.getPath + "/road")
    travelRouteDf.repartition(1).write.json(rootDir.getPath + "/travelRoute")

    val json = createTestConfig(rootDir)

    Files.write(Paths.get(rootDir.getPath, "config.json"), json.getBytes(UTF_8))
    val graphSource = GraphSource.json(spark)
    val graph = graphSource.loadGraph(Paths.get(testDir.getPath, "json", "config.json"))

    runTestOn(graph)
  }

  test("A ParquetGraphSource infers the graph data correctly") {
    val rootDir = newDir("parquet")
    peopleDf.repartition(1).write.parquet(rootDir.getPath + "/person")
    cityDf.repartition(1).write.parquet(rootDir.getPath + "/city")
    bornInDf.repartition(1).write.parquet(rootDir.getPath + "/bornIn")
    roadDf.repartition(1).write.parquet(rootDir.getPath + "/road")
    travelRouteDf.repartition(1).write.parquet(rootDir.getPath + "/travelRoute")

    val json = createTestConfig(rootDir)

    Files.write(Paths.get(rootDir.getPath, "config.json"), json.getBytes(UTF_8))
    val graphSource = GraphSource.parquet(spark)
    val graph = graphSource.loadGraph(Paths.get(testDir.getPath, "parquet", "config.json"))

    runTestOn(graph)
  }

  private def createTestConfig(rootDir: File): String =
    s"""
       |{
       |  "graph_name": "test_graph",
       |  "graph_root_dir": "${rootDir.getPath}",
       |  "vertex_labels": ["person", "city"],
       |  "edge_labels": ["bornIn", "road"],
       |  "path_labels": ["travelRoute"],
       |  "edge_restrictions": [
       |    {
       |      "conn_label": "BornIn",
       |      "source_label": "Person",
       |      "destination_label": "City"
       |    },
       |    {
       |      "conn_label": "Road",
       |      "source_label": "City",
       |      "destination_label": "City"
       |    }
       |  ],
       |  "path_restrictions": [
       |    {
       |      "conn_label": "TravelRoute",
       |      "source_label": "City",
       |      "destination_label": "City"
       |    }
       |  ]
       |} """.stripMargin

  private def runTestOn(graph: SparkGraph): Unit = {
    // It would be nicer to just call compareDfs here, but upon reading from file, Spark sometimes
    // changes the order in which the columns of the tableName have been written (also by Spark) in that
    // file, making intersect and except calls to fail. We impose a column order on the DataFrame by
    // selecting the columns as we wish.
    comparePerson(graph.tableMap(Label("person")).data)
    compareCity(graph.tableMap(Label("city")).data)
    compareBornIn(graph.tableMap(Label("bornIn")).data)
    compareRoad(graph.tableMap(Label("road")).data)
    compareTravelRoute(graph.tableMap(Label("travelRoute")).data)
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
}
