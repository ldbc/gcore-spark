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

package construct

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
class MultipleTemplateTests extends FunSuite {
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

  test("3.2.1 Multiple nodes construct"){
    val query = "CONSTRUCT (n), (m) MATCH (n)-[e:HasInterest]->(m)"
    val expectedPerson = Seq(
      ("104"), ("103")
    ).toDF("id")
    val expectedTag = Seq(
      ("106")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)
  }

  test("3.2.2 Node and edge construct"){
    val query = "CONSTRUCT (n)-[e]->(m), (q) MATCH (n)-[e:HasInterest]->(m), (q:Place)"
    val expectedPerson = Seq(
      ("104"), ("103")
    ).toDF("id")
    val expectedTag = Seq(
      ("106")
    ).toDF("id")
    val expectedPlace = Seq(
      ("105"),("115")
    ).toDF("id")

    val rewrited: AlgebraTreeNode = rewriter(parser(query))
    val graph: PathPropertyGraph = target(rewrited)

    val resultPerson: DataFrame = graph.tableMap(Label("Person")).asInstanceOf[Table[DataFrame]].data
    assert(resultPerson.select("id").except(expectedPerson).count == 0)

    val resultTag: DataFrame = graph.tableMap(Label("Tag")).asInstanceOf[Table[DataFrame]].data
    assert(resultTag.select("id").except(expectedTag).count == 0)

    val resultPlace: DataFrame = graph.tableMap(Label("Place")).asInstanceOf[Table[DataFrame]].data
    assert(resultPlace.select("id").except(expectedPlace).count == 0)
  }
}
