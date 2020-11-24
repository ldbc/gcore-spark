/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - Universidad de Talca (www.utalca.cl) 2020
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

import java.sql.{Date, Timestamp}

import algebra.expressions.Label
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.EntitySchema.LabelRestrictionMap
import schema.{SchemaMap, Table}
import spark.SparkGraph

case class BasicGraph(spark: SparkSession) extends SparkGraph {

  //Nodes
  val frank = Person2(id = 100, firstName = "Frank",lastName = "Gold", employer = "[MIT][CWI]", university = "Harvard")
  val john = Person2(id = 101, firstName = "John", lastName = "Doe", employer = "Acme", university = "Oxford")
  val peter = Person2(id = 102, firstName = "Peter", lastName = "Smith", employer = null, university = "Stanford")
  val celine = Person2(id = 103, firstName = "Celine", lastName = "Mayer", employer = "HAL", university = "Harvard")
  val alice = Person2(id = 104, firstName = "Alice", lastName = "Hoffman", employer = "Acme", university = "Yale")

  val houston = Place(id = 105, name = "Houston", founded = new Date(java.util.Calendar.getInstance().getTime.getTime), timeStamp=  new Timestamp(java.util.Calendar.getInstance().getTimeInMillis) )

  val wagner = Tag(id = 106, name = "Wagner")


  //Edges
  val frankKnowsJohn = Knows2(id = 200, fromId = 100, toId = 101, nr_messages = 0)
  val johnKnowsPeter = Knows2(id = 201, fromId = 101, toId = 102, nr_messages = 0)
  val peterKnowsCeline = Knows2(id = 202, fromId = 102, toId = 103, nr_messages = 0)
  val aliceKnowsFrank = Knows2(id = 203, fromId = 104, toId = 100, nr_messages = 0)
  val frankKnowsPeter = Knows2(id = 204, fromId = 100, toId = 102, nr_messages = 2)
  val peterKnowsAlice = Knows2(id = 205, fromId = 102, toId = 104, nr_messages = 3)

  val johnKnowsFrank = Knows2(id = 206, fromId = 101, toId = 100, nr_messages = 0)
  val peterKnowsJohn = Knows2(id = 207, fromId = 102, toId = 101, nr_messages = 0)
  val celineKnowsPeter = Knows2(id = 208, fromId = 103, toId = 102, nr_messages = 0)
  val frankKnowsAlice = Knows2(id = 209, fromId = 100, toId = 104, nr_messages = 0)
  val peterKnowsFrank = Knows2(id = 210, fromId = 102, toId = 100, nr_messages = 2)
  val aliceKnowsPeter = Knows2(id = 211, fromId = 104, toId = 102, nr_messages = 3)


  val johnIsLocatedInHouston= IsLocatedIn(id = 400, fromId = 101, toId = 105)
  val celineIsLocatedInHouston= IsLocatedIn(id = 401, fromId = 103, toId = 105)
  val aliceIsLocatedInHouston= IsLocatedIn(id = 402, fromId = 104, toId = 105)

  val johnIsLocatedInHoustonInverse= IsLocatedIn2(id = "403", fromId = 105, toId = 101)
  val celineIsLocatedInHoustonInverse= IsLocatedIn2(id = "404", fromId = 105, toId = 103)
  val aliceIsLocatedInHoustonInverse= IsLocatedIn2(id = "405", fromId = 105, toId = 104)

  val celineHasInterestInWagner = HasInterest(id = 500 , fromId = 103, toId = 106)
  val aliceHasInterestInWagner = HasInterest(id = 501 , fromId = 104, toId = 106)

  val celineHasInterestInWagnerInverse = HasInterest2(id = "502" , fromId = 106, toId = 103)
  val aliceHasInterestInWagnerInverse = HasInterest2(id = "503" , fromId = 106, toId = 104)

  //path
  val toAlice = ToWagner(700, 2, 101, 104, Seq(201, 205))
  val toCeline = ToWagner(701, 2, 101, 103, Seq(201, 202))

  import spark.implicits._

  override var graphName: String = "basic_graph"

  override def vertexData: Seq[Table[DataFrame]] =
    Seq(
      Table(Label("Person"), Seq(frank, john, peter, celine, alice).toDF()),
      Table(Label("Place"), Seq(houston).toDF()),
      Table(Label("Tag"), Seq(wagner).toDF())
    )

  override def edgeData: Seq[Table[DataFrame]] =
    Seq(
      Table(Label("Knows"),Seq(frankKnowsJohn, johnKnowsPeter, peterKnowsCeline, aliceKnowsFrank, frankKnowsPeter,peterKnowsAlice,
        johnKnowsFrank,peterKnowsJohn,celineKnowsPeter,frankKnowsAlice,peterKnowsFrank, aliceKnowsPeter).toDF()),
      Table(Label("IsLocatedIn"),Seq(johnIsLocatedInHouston, celineIsLocatedInHouston, aliceIsLocatedInHouston
      ).toDF()),
      Table(Label("HasInterest"),Seq(celineHasInterestInWagner, aliceHasInterestInWagner
      ).toDF()),
      Table(Label("IsLocatedIn2"),Seq( johnIsLocatedInHoustonInverse, celineIsLocatedInHoustonInverse,
        aliceIsLocatedInHoustonInverse).toDF()),
      Table(Label("HasInterest2"),Seq( celineHasInterestInWagnerInverse, aliceHasInterestInWagnerInverse).toDF())
    )

  override def pathData: Seq[Table[DataFrame]] = Seq(Table(Label("ToWagner"), Seq(toCeline,toAlice).toDF()))

  override def edgeRestrictions: LabelRestrictionMap =
    SchemaMap(Map(
      Label("Knows") -> (Label("Person"), Label("Person")),
      Label("IsLocatedIn") -> (Label("Person"), Label("Place")),
      Label("HasInterest") -> (Label("Person"), Label("Tag")),
      Label("IsLocatedIn2") -> (Label("Place"), Label("Person")),
      Label("HasInterest2") -> (Label("Tag"), Label("Person"))
    ))

  override def storedPathRestrictions: LabelRestrictionMap = SchemaMap(Map(Label("ToWagner") -> (Label("Person"), Label("Tag"))))
}
/*//Nodes
sealed case class Person2(id: Int, firstName: String, lastName: String, employer: String, university: String)
sealed case class Place(id: Int, name: String,founded: Date, timeStamp: Timestamp)
sealed case class Tag(id: Int, name: String)
sealed case class Manager(id: Int, firstName: String, lastName: String, employer: String)
//Edges
sealed case class Knows2(id: Int, fromId: Int, toId: Int,nr_messages: Int)
sealed case class IsLocatedIn(id: Int, fromId: Int, toId: Int)
sealed case class HasInterest(id: Int, fromId: Int, toId: Int)

//Path
sealed case class ToWagner(id: Int, hops: Int, fromId: Int, toId: Int, edges: Seq[Int])
*/
sealed case class IsLocatedIn2(id: String, fromId: Int, toId: Int)
sealed case class HasInterest2(id: String, fromId: Int, toId: Int)