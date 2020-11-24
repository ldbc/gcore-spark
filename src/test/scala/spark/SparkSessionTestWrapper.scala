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

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Provides the [[SparkSession]] for a Spark test. */
trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("G-CORE interpreter test suite")
      .getOrCreate()
  }

  def compareDfs(actual: DataFrame, expected: DataFrame): Unit = {
    assert(actual.except(expected).count() == 0)
    assert(expected.except(actual).count() == 0)
  }

  def compareHeaders(expectedHeader: Seq[String], actualDf: DataFrame): Unit = {
    val actualHeader = actualDf.columns
    assert(actualHeader.length == expectedHeader.length)
    assert(actualHeader.toSet == expectedHeader.toSet)
  }
}
