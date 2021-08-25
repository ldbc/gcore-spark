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

package algebra.trees

import algebra.exceptions.AmbiguousGraphForExistentialPatternException
import algebra.expressions.{Exists, ObjectPattern, Reference, True}
import algebra.operators.SimpleMatchClause
import algebra.types._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import schema.Catalog

@RunWith(classOf[JUnitRunner])
class AddGraphToExistentialPatternsTest extends FunSuite {

  private val graph1: NamedGraph = NamedGraph("graph_1")
  private val graph2: NamedGraph = NamedGraph("graph_2")
  private val bindingToGraph: Map[Reference, Graph] =
    Map(Reference("u") -> graph1, Reference("v") -> graph1, Reference("z") -> graph2)

  private val rewriter: AddGraphToExistentialPatterns =
    AddGraphToExistentialPatterns(AlgebraContext(Catalog.empty, Some(bindingToGraph)))

  private val emptyObjPattern: ObjectPattern = ObjectPattern(True, True)

  test("Each GraphPattern in Exists becomes a SimpleMatchClause") {
    val conn1 = Vertex(Reference("u"), emptyObjPattern)
    val conn2 = Vertex(Reference("w"), emptyObjPattern)
    val graphPattern = GraphPattern(topology = Seq(conn1, conn2))
    val exists = Exists(graphPattern)

    val expected =
      Set(
        SimpleMatchClause(GraphPattern(Seq(conn1)), graph1),
        SimpleMatchClause(GraphPattern(Seq(conn2)), DefaultGraph)
      )

    val actual = rewriter rewriteTree exists

    assert(actual.children.size == 2)
    assert(actual.children.toSet == expected)
  }

  test("Exists on mapped vertex - (u) in graph_1") {
    val conn = Vertex(Reference("u"), emptyObjPattern)
    runSingleConnTest(conn, graph1)
  }

  test("Exists on un-mapped vertex - (w) in default graph") {
    val conn = Vertex(Reference("w"), emptyObjPattern)
    runSingleConnTest(conn, DefaultGraph)
  }

  test("Exists on mapped edge - (u)->(v) in graph_1") {
    val left = Vertex(Reference("u"), emptyObjPattern)
    val right = Vertex(Reference("v"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn, emptyObjPattern)
    runSingleConnTest(conn, graph1)
  }

  test("Exists on un-mapped edge - (x)->(y) in default graph") {
    val left = Vertex(Reference("x"), emptyObjPattern)
    val right = Vertex(Reference("y"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn, emptyObjPattern)
    runSingleConnTest(conn, DefaultGraph)
  }

  test("Exists on edge with one endpoint mapped will take mapped endpoint's graph - " +
    "(u)->(y), (u) in graph_1, (y) in default graph") {
    val left = Vertex(Reference("u"), emptyObjPattern)
    val right = Vertex(Reference("y"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn, emptyObjPattern)
    runSingleConnTest(conn, graph1)
  }

  test("Exists on edge with differently mapped endpoints will throw error - " +
    "(u)->(z), (u) in graph_1, (z) in graph_2") {
    val left = Vertex(Reference("u"), emptyObjPattern)
    val right = Vertex(Reference("z"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn, emptyObjPattern)
    val graphPattern = GraphPattern(topology = Seq(conn))
    val exists = Exists(graphPattern)

    assertThrows[AmbiguousGraphForExistentialPatternException] {
      rewriter rewriteTree exists
    }
  }

  private def runSingleConnTest(conn: Connection, expectedGraph: Graph): Unit = {
    val graphPattern = GraphPattern(topology = Seq(conn))
    val exists = Exists(graphPattern)
    val expected = SimpleMatchClause(graphPattern, expectedGraph)
    val actual = rewriter rewriteTree exists
    assert(actual.children.head == expected)
  }
}
