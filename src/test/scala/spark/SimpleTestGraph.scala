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

/** An example graph for testing. */
trait SimpleTestGraph {

  val peopleList =
    List(
      Person(id = 100, name = "Daenerys Targaryen", age = 14, isAlive = true),
      Person(id = 101, name = "Tyrion Lannister", age = 25, isAlive = true))

  val cityList =
    List(
      City(id = 102, name = "King's Landing"),
      City(id = 103, name = "Casterly Rock"),
      City(id = 104, name = "Dragonstone"),
      City(id = 105, name = "Pentos"),
      City(id = 106, name = "Vaes Dothrak"),
      City(id = 107, name = "Qarth"),
      City(id = 108, name = "Meeren"))

  val bornInList =
    List(
      BornIn(id = 201, fromId = 100, toId = 104, hasLeft = true),
      BornIn(id = 202, fromId = 101, toId = 103, hasLeft = true))

  val roadList =
    List(
      Road(id = 203, fromId = 103, toId = 102),
      Road(id = 204, fromId = 104, toId = 105),
      Road(id = 205, fromId = 105, toId = 106),
      Road(id = 206, fromId = 106, toId = 107),
      Road(id = 207, fromId = 107, toId = 108))

  val travelRouteList =
    List(
      TravelRoute(id = 301, fromId = 103, toId = 102, edges = Seq(203)),
      TravelRoute(id = 302, fromId = 104, toId = 108, edges = Seq(204, 205, 206, 207)))
}

sealed case class Person(id: Int, name: String, age: Int, isAlive: Boolean)
sealed case class City(id: Int, name: String)
sealed case class BornIn(id: Int, fromId: Int, toId: Int, hasLeft: Boolean)
sealed case class Road(id: Int, fromId: Int, toId: Int)
sealed case class TravelRoute(id: Int, fromId: Int, toId: Int, edges: Seq[Int])
