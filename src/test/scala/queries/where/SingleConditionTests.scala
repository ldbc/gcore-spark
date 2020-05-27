/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - Millennium Institute for Foundational Research on Data (Chile) (2019-2020)
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

package where

import algebra.AlgebraRewriter
import algebra.expressions.Label
import algebra.trees.{AlgebraContext, AlgebraTreeNode}
import compiler.{CompileContext, ParseStage, RewriteStage, RunTargetCodeStage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import parser.SpoofaxParser
import parser.trees.ParseContext
import schema.{PathPropertyGraph, Table}
import spark.SparkCatalog
import spark.examples.SocialTestGraph
import spark.sql.SqlRunner

@RunWith(classOf[JUnitRunner])
class SingleConditionTests extends FunSuite{
  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("G-CORE Runner")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._
  val catalog: SparkCatalog = SparkCatalog(sparkSession)
  catalog.registerGraph(SocialTestGraph(sparkSession))
  catalog.setDefaultGraph("social_test_graph")
  val context = CompileContext(catalog, sparkSession)
  val parser: ParseStage = SpoofaxParser(ParseContext(context.catalog))
  val rewriter: RewriteStage = AlgebraRewriter(AlgebraContext(context.catalog))
  val target: RunTargetCodeStage = SqlRunner(context)

  test("2.1.1 Integer check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Message) WHERE n.length = 50"
    val expected = Seq(
      ("107",50,true)
    ).toDF("id","length","important")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id","length","important").except(expected).count == 0)
  }

  test("2.1.2 Integer check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:Knows]->(m:Person) WHERE e.nr_messages=3"
    val expectedPerson = Seq(
      ("Hoffman","104","Acme","Alice","Yale"),
      ("Smith","102",null,"Peter","Stanford")
    ).toDF("lastName", "id","employer","firstName","university")
    val expectedEdge = Seq(
      ("205","102","104","3"),
      ("211","104","102","3")
    ).toDF("id","fromId","toId","nr_messages")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("lastName", "id","employer","firstName","university").except(expectedPerson).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Knows")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id","fromId","toId","nr_messages").except(expectedEdge).count == 0)
  }

  test("2.1.3 String check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.lastName='Hoffman'"
    val expected = Seq(
      ("Hoffman","104","Acme","Alice","Yale")
    ).toDF("lastName", "id","employer","firstName","university")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("lastName", "id","employer","firstName","university").except(expected).count == 0)
  }

  test("2.1.4 String check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:IsLocatedIn]->(m:Place) WHERE e.street = 'Av 14'"
    val expectedPerson = Seq(
      ("Mayer","103","HAL","Celine","Harvard")
    ).toDF("lastName", "id","employer","firstName","university")
    val expectedPlace = Seq(
      ("105","Houston")
    ).toDF("id","name")
    val expectedEdge = Seq(
      ("401", "103", "105")
    ).toDF("id", "fromId", "toId")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("lastName", "id","employer","firstName","university").except(expectedPerson).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id","name").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id", "fromId", "toId").except(expectedEdge).count == 0)
  }

  test("2.1.5 Boolean check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Message) WHERE n.important = true"
    val expected = Seq(
      ("107"), ("109"), ("110"), ("112")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Message")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.1.6 Boolean check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:IsLocatedIn]->(m:Place) WHERE e.bool = false"
    val expectedPerson = Seq(
      ("101"), ("104")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105")
    ).toDF("id")
    val expectedEdge = Seq(
      ("400"), ("402")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    //Place also has a timestamp field that is filled at execution time
    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.1.7 Date check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Place) WHERE n.founded = '2020-01-01'"
    val expected = Seq(
      ("105")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.1.8 Date check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:HasInterest]->(m:Tag) WHERE e.since = '2020-01-31'"
    val expectedPerson = Seq(
      ("103")
    ).toDF("id")
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedEdge = Seq(
      ("500")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("HasInterest")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.1.9 Timestamp check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Place) WHERE n.timeStamp = '2020-01-01 10:00:00'"
    val expected = Seq(
      ("105")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.1.10 Timestamp check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:HasInterest]->(m:Tag) WHERE e.timestamp = '2020-01-31 15:00:00'"
    val expectedPerson = Seq(
      ("103")
    ).toDF("id")
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedEdge = Seq(
      ("500")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("HasInterest")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.1.11 Null check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer IS NULL"
    val expected = Seq(
      ("102")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.1.12 Null check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:IsLocatedIn]->(m:Place) WHERE e.street IS NULL"
    val expectedPerson = Seq(
      ("104")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105")
    ).toDF("id")
    val expectedEdge = Seq(
      ("402")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.1.13 Not null check on node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer IS NOT NULL"
    val expected = Seq(
      ("100"), ("101"), ("103"), ("104")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.1.14 Not null check on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:IsLocatedIn]->(m:Place) WHERE e.street IS NOT NULL"
    val expectedPerson = Seq(
      ("101"), ("103")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105")
    ).toDF("id")
    val expectedEdge = Seq(
      ("400"),("401")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.1.15 NOT Operator on node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE NOT n.employer = 'Acme'"
    val expected = Seq(
      ("100"), ("102"), ("103")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.1.16 NOT Operator on edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n)-[e:IsLocatedIn]->(m) WHERE NOT e.bool = 'true'"
    val expectedPerson = Seq(
      ("101"), ("104")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105")
    ).toDF("id")
    val expectedEdge = Seq(
      ("400"),("402")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("IsLocatedIn")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }
}
