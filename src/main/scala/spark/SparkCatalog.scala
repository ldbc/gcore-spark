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

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.{Catalog, PathPropertyGraph}

/** A [[Catalog]] for [[PathPropertyGraph]]s backed by [[DataFrame]]s. */
case class SparkCatalog(sparkSession: SparkSession) extends Catalog {

  override type StorageType = DataFrame

  val jsonSource: JsonGraphSource = GraphSource.json(sparkSession)
  val parquetSource: ParquetGraphSource = GraphSource.parquet(sparkSession)

  /**
    * Register a graph coming from a [[GraphSource]] and configured through the config file stored
    * under the given path.
    */
  // TODO: Cache already registered graphs.
  def registerGraph(graphSource: GraphSource, configPath: Path): Unit = {
    val graph = graphSource.loadGraph(configPath)
    super.registerGraph(graph)
  }
}
