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

package `match`

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
class OptionalMatchTests extends FunSuite {
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
}
