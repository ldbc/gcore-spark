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
class MultipleConditionTests extends FunSuite {
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

  test("2.2.1 AND Operator on Node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer='Acme' AND n.university='Yale'"
    val expected = Seq(
      ("104")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.2 AND Operator on Edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:Knows]->(m:Person) WHERE e.nr_messages='3' AND e.fromId='102'"
    val expectedPerson = Seq(
      ("104"), ("102")
    ).toDF("id")
    val expectedEdge = Seq(
      ("205")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Knows")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.2.3 OR Operator on Node"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer='Acme' OR n.employer='HAL'"
    val expected = Seq(
      ("101"), ("103"), ("104")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.4 OR Operator on Edge"){
    val query = "CONSTRUCT (n)-[e]->(m) MATCH (n:Person)-[e:Knows]->(m:Person) WHERE e.nr_messages='3' OR e.nr_messages='2'"
    val expectedPerson = Seq(
      ("104"), ("102"), ("100")
    ).toDF("id")
    val expectedEdge = Seq(
      ("204"), ("205"), ("210"), ("211")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultEdge: DataFrame = graph.tableMap(Label("Knows")).asInstanceOf[Table[DataFrame]].data
    assert(resultEdge.select("id").except(expectedEdge).count == 0)
  }

  test("2.2.5 AND Operator with IS NULL Operator"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer IS NULL AND n.university = 'Stanford'"
    val expected = Seq(
      ("102")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.6 AND Operator with IS NOT NULL Operator"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer IS NOT NULL AND n.university = 'Harvard'"
    val expected = Seq(
      ("100"),("103")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.7 OR Operator with IS NULL Operator"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer IS NULL OR n.employer = 'HAL'"
    val expected = Seq(
      ("102"),("103")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.8 OR Operator with IS NOT NULL Operator"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer IS NOT NULL OR n.university = 'Stanford'"
    val expected = Seq(
      ("100"),("101"),("102"),("103"),("104")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.9 AND Operator with NOT Operator"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer = 'Acme' AND NOT n.university = 'Yale'"
    val expected = Seq(
      ("101")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }

  test("2.2.10 OR Operator with NOT Operator"){
    val query = "CONSTRUCT (n) MATCH (n:Person) WHERE n.employer = 'Acme' OR NOT n.university = 'Harvard'"
    val expected = Seq(
      ("101"),("102"),("104")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)
    val result: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(result.select("id").except(expected).count == 0)
  }
}
