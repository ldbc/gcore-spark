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

import compiler.{CompileContext, Compiler, GcoreCompiler}
import org.apache.spark.sql.SparkSession
import schema.Catalog
import spark.SparkCatalog
import spark.examples.{DummyGraph, PeopleGraph}

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
    gcoreRunner.catalog.setDefaultGraph("people_graph")

    gcoreRunner.compiler.compile(
      """
        | CONSTRUCT (a :ALabel)-[e0 :e0Label]->(x GROUP p.employer :XLabel {cnt := COUNT(*)})
        | MATCH (c:Company)<-[e]-(p:Person)
      """.stripMargin)
  }
}

case class GcoreRunner(sparkSession: SparkSession, compiler: Compiler, catalog: Catalog)
