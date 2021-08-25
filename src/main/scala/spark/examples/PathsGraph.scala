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

case class PathsGraph(spark: SparkSession) extends SparkGraph {

  val A = Blue(0, "A")
  val B = Blue(1, "B")
  val C = Blue(2, "C")
  val D = Blue(3, "D")
  val E = Blue(4, "E")
  val F = Blue(5, "F")
  val G = Blue(6, "G")
  val H = Blue(7, "H")

  val AB = Knows(10, 0, 1)
  val AE = Knows(11, 0, 4)
  val BC = Knows(12, 1, 2)
  val BE = Knows(13, 1, 4)
  val CD = Knows(14, 2, 3)
  val DE = Knows(15, 3, 4)
  val DF = Knows(16, 3, 5)
  val ED = Knows(17, 4, 3)
  val EG = Knows(18, 4, 6)
  val GA = Knows(19, 6, 0)

  val AF = RelatedTo(20, 0, 5)
  val GD = RelatedTo(22, 6, 3)

  import spark.implicits._

  override var graphName: String = "paths_graph"

  override def vertexData: Seq[Table[DataFrame]] = Seq(
    Table(
      name = Label("Blue"),
      data = Seq(A, B, C, D, E, F, G, H).toDF)
  )

  override def edgeData: Seq[Table[DataFrame]] = Seq(
    Table(
      name = Label("knows"),
      data = Seq(AB, AE, BC, BE, CD, DE, DF, ED, EG, GA).toDF),
    Table(
      name = Label("relatedTo"),
      data = Seq(AF, GD).toDF)
  )

  override def pathData: Seq[Table[DataFrame]] = Seq.empty

  override def edgeRestrictions: LabelRestrictionMap = SchemaMap(Map(
    Label("knows") -> (Label("Blue"), Label("Blue")),
    Label("relatedTo") -> (Label("Blue"), Label("Blue"))
  ))

  override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty
}

sealed case class Blue(id: Int, name: String)
sealed case class Grey(id: Int, name: String)
sealed case class Knows(id: Int, fromId: Int, toId: Int)
sealed case class RelatedTo(id: Int, fromId: Int, toId: Int)
