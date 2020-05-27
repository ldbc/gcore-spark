/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - Universidad de Talca (2018)
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

/**
  * Copy of social_graph with a few modifications. Only for testing
  */
case class SocialTestGraph(spark: SparkSession) extends SparkGraph {
  //Nodes
  val frank = TestPerson(id = 100, firstName = "Frank",lastName = "Gold", employer = "[MIT][CWI]", university = "Harvard")
  val john = TestPerson(id = 101, firstName = "John", lastName = "Doe", employer = "Acme", university = "Oxford")
  val peter = TestPerson(id = 102, firstName = "Peter", lastName = "Smith", employer = null, university = "Stanford")
  val celine = TestPerson(id = 103, firstName = "Celine", lastName = "Mayer", employer = "HAL", university = "Harvard")
  val alice = TestPerson(id = 104, firstName = "Alice", lastName = "Hoffman", employer = "Acme", university = "Yale")

  val houston = TestPlace(id = 105, name = "Houston", founded = Date.valueOf("2020-01-01"), timeStamp=  Timestamp.valueOf("2020-01-01 10:00:00") )
  val newYork = TestPlace(id = 115, name = "New York", founded = Date.valueOf("2020-01-15"), timeStamp = Timestamp.valueOf("2020-01-15 11:00:00"))

  val wagner = TestTag(id = 106, name = "Wagner")

  val message7= TestMessage(id = 107, length = 50, important = true)
  val message8= TestMessage(id =108, length=98, important=false)
  val message9= TestMessage(id =109, length=147, important=true)
  val message10= TestMessage(id =110, length=65, important=true)
  val message11= TestMessage(id =111, length=110, important=false)
  val message12= TestMessage(id =112, length=76, important=true)
  val message13= TestMessage(id =113, length=110, important=false)

  val manager = TestManager(id = 114, firstName = "Celine", lastName = "Mayer", employer = "HAL")


  //Edges
  val frankKnowsJohn = TestKnows(id = 200, fromId = 100, toId = 101, nr_messages = 0)
  val johnKnowsPeter = TestKnows(id = 201, fromId = 101, toId = 102, nr_messages = 0)
  val peterKnowsCeline = TestKnows(id = 202, fromId = 102, toId = 103, nr_messages = 0)
  val aliceKnowsFrank = TestKnows(id = 203, fromId = 104, toId = 100, nr_messages = 0)
  val frankKnowsPeter = TestKnows(id = 204, fromId = 100, toId = 102, nr_messages = 2)
  val peterKnowsAlice = TestKnows(id = 205, fromId = 102, toId = 104, nr_messages = 3)

  val johnKnowsFrank = TestKnows(id = 206, fromId = 101, toId = 100, nr_messages = 0)
  val peterKnowsJohn = TestKnows(id = 207, fromId = 102, toId = 101, nr_messages = 0)
  val celineKnowsPeter = TestKnows(id = 208, fromId = 103, toId = 102, nr_messages = 0)
  val frankKnowsAlice = TestKnows(id = 209, fromId = 100, toId = 104, nr_messages = 0)
  val peterKnowsFrank = TestKnows(id = 210, fromId = 102, toId = 100, nr_messages = 2)
  val aliceKnowsPeter = TestKnows(id = 211, fromId = 104, toId = 102, nr_messages = 3)


  val msg7replyOf9 = TestReplyOf(id= 300, fromId= 109, toId= 107)
  val msg10replyOf7 = TestReplyOf(id= 301, fromId= 107, toId= 110)
  val msg8replyOf10 = TestReplyOf(id= 302, fromId= 110, toId= 108)
  val msg12replyOf13 = TestReplyOf(id= 303, fromId= 113, toId= 112)
  val msg11replyOf12 = TestReplyOf(id= 304, fromId= 112, toId= 111)

  val johnIsLocatedInHouston= TestIsLocatedIn(id = 400, fromId = 101, toId = 105, street = "Av 15", bool=false)
  val celineIsLocatedInHouston= TestIsLocatedIn(id = 401, fromId = 103, toId = 105, street = "Av 14", bool=true)
  val aliceIsLocatedInHouston= TestIsLocatedIn(id = 402, fromId = 104, toId = 105, street = null, bool=false)

  val celineHasInterestInWagner = TestHasInterest(id = 500 , fromId = 103, toId = 106, since = Date.valueOf("2020-01-31"), timestamp = Timestamp.valueOf("2020-01-31 15:00:00"))
  val aliceHasInterestInWagner = TestHasInterest(id = 501 , fromId = 104, toId = 106, since = Date.valueOf("2020-01-28"), timestamp = Timestamp.valueOf("2020-01-28 14:00:00"))

  val msg12HasCreatorFrank = TestHasCreator(id = 600, fromId = 100, toId = 112)
  val msg9HasCreatorPeter = TestHasCreator(id = 601, fromId = 102, toId = 109)
  val msg10HasCreatorPeter = TestHasCreator(id = 602, fromId = 102, toId = 110)
  val msg11HasCreatorPeter = TestHasCreator(id = 603, fromId = 102, toId = 111)
  val msg13HasCreatorPeter = TestHasCreator(id = 604, fromId = 102, toId = 113)
  val msg7HasCreatorAlice = TestHasCreator(id = 605, fromId = 104, toId = 107)
  val msg8HasCreatorAlice = TestHasCreator(id = 606, fromId = 104, toId = 108)

  //path
  val toAlice = TestToWagner(700, 2, 101, 104, Seq(201, 205))
  val toCeline = TestToWagner(701, 2, 101, 103, Seq(201, 202))

  import spark.implicits._

  override var graphName: String = "social_test_graph"

  override def vertexData: Seq[Table[DataFrame]] =
    Seq(
      Table(Label("Person"), Seq(frank, john, peter, celine, alice).toDF()),
      Table(Label("Message"), Seq(message7, message8, message9, message10, message11, message12, message13).toDF()),
      Table(Label("Place"), Seq(houston, newYork).toDF()),
      Table(Label("Tag"), Seq(wagner).toDF()),
      Table(Label("Manager"), Seq(manager).toDF())
    )

  override def edgeData: Seq[Table[DataFrame]] =
    Seq(
      Table(Label("Knows"),Seq(frankKnowsJohn, johnKnowsPeter, peterKnowsCeline, aliceKnowsFrank, frankKnowsPeter,peterKnowsAlice,
        johnKnowsFrank,peterKnowsJohn,celineKnowsPeter,frankKnowsAlice,peterKnowsFrank, aliceKnowsPeter).toDF()),
      Table(Label("ReplyOf"),Seq(msg7replyOf9, msg10replyOf7, msg8replyOf10, msg12replyOf13, msg11replyOf12).toDF()),
      Table(Label("IsLocatedIn"),Seq(johnIsLocatedInHouston, celineIsLocatedInHouston, aliceIsLocatedInHouston).toDF()),
      Table(Label("HasInterest"),Seq(celineHasInterestInWagner, aliceHasInterestInWagner).toDF()),
      Table(Label("HasCreator"),Seq(msg12HasCreatorFrank, msg9HasCreatorPeter,msg10HasCreatorPeter,msg11HasCreatorPeter,
        msg13HasCreatorPeter,msg7HasCreatorAlice,msg8HasCreatorAlice).toDF())

    )

  override def pathData: Seq[Table[DataFrame]] = Seq(Table(Label("ToWagner"), Seq(toCeline,toAlice).toDF()))

  override def edgeRestrictions: LabelRestrictionMap =
    SchemaMap(Map(
      Label("Knows") -> (Label("Person"), Label("Person")),
      Label("ReplyOf") -> (Label("Message"), Label("Message")),
      Label("IsLocatedIn") -> (Label("Person"), Label("Place")),
      Label("HasInterest") -> (Label("Person"), Label("Tag")),
      Label("HasCreator") -> (Label("Person"), Label("Message"))
    ))

  override def storedPathRestrictions: LabelRestrictionMap = SchemaMap(Map(Label("ToWagner") -> (Label("Person"), Label("Tag"))))
}
//Nodes
sealed case class TestPerson(id: Int, firstName: String, lastName: String, employer: String, university: String)
sealed case class TestMessage(id: Int, length: Int, important: Boolean)
sealed case class TestPlace(id: Int, name: String,founded: Date, timeStamp: Timestamp)
sealed case class TestTag(id: Int, name: String)
sealed case class TestManager(id: Int, firstName: String, lastName: String, employer: String)
//Edges
sealed case class TestKnows(id: Int, fromId: Int, toId: Int,nr_messages: Int)
sealed case class TestReplyOf(id: Int, fromId: Int, toId: Int)
sealed case class TestIsLocatedIn(id: Int, fromId: Int, toId: Int, street: String, bool: Boolean)
sealed case class TestHasInterest(id: Int, fromId: Int, toId: Int, since: Date, timestamp: Timestamp)
sealed case class TestHasCreator(id: Int, fromId: Int, toId: Int)

//Path
sealed case class TestToWagner(id: Int, hops: Int, fromId: Int, toId: Int, edges: Seq[Int])
