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

package spark.examples

import algebra.expressions.Label
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.EntitySchema.LabelRestrictionMap
import schema.{SchemaMap, Table}
import spark.SparkGraph

case class PeopleGraph(spark: SparkSession) extends SparkGraph {

  val frankMit = Person(id = 100, name = "Frank", employer = "MIT")
  val frankCwi = Person(id = 101, name = "Frank", employer = "CWI")
  val alice = Person(id = 102, name = "Alice", employer = "Acme")
  val celine = Person(id = 103, name = "Celine", employer = "HAL")
  val john = Person(id = 104, name = "John", employer = "Acme")

  val mit = Company(id = 105, name = "MIT")
  val cwi = Company(id = 106, name = "CWI")
  val acme = Company(id = 107, name = "Acme")
  val hal = Company(id = 108, name = "HAL")

  val frankWorksAtMit = WorksAt(id = 200, fromId = 100, toId = 105)
  val frankWorksAtCwi = WorksAt(id = 201, fromId = 101, toId = 106)
  val aliceWorksAtAcme = WorksAt(id = 202, fromId = 102, toId = 107)
  val celineWorksAtHal = WorksAt(id = 203, fromId = 103, toId = 108)
  val johnWorksAtAcme = WorksAt(id = 204, fromId = 104, toId = 107)

  import spark.implicits._

  override def graphName: String = "people_graph"

  override def vertexData: Seq[Table[DataFrame]] =
    Seq(
      Table(Label("Person"), Seq(frankCwi, frankMit, alice, celine, john).toDF()),
      Table(Label("Company"), Seq(mit, cwi, acme, hal).toDF())
    )

  override def edgeData: Seq[Table[DataFrame]] =
    Seq(
      Table(
        Label("WorksAt"),
        Seq(frankWorksAtCwi, frankWorksAtMit, aliceWorksAtAcme, celineWorksAtHal, johnWorksAtAcme)
          .toDF())
    )

  override def pathData: Seq[Table[DataFrame]] = Seq.empty

  override def edgeRestrictions: LabelRestrictionMap =
    SchemaMap(Map(
      Label("WorksAt") -> (Label("Person"), Label("Company"))
    ))

  override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty
}

sealed case class Person(id: Int, name: String, employer: String)
sealed case class Company(id: Int, name: String)
sealed case class WorksAt(id: Int, fromId: Int, toId: Int)
