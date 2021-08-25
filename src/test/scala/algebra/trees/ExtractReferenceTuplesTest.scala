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

import algebra.expressions.{ObjectPattern, Reference, True}
import algebra.trees.ExtractReferenceTuples.extractReferenceTuples
import algebra.types._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractReferenceTuplesTest extends FunSuite {

  private val emptyObjPattern: ObjectPattern = ObjectPattern(True, True)

  test("Missing connections become empty Set") {
    val algebraTree = GraphPattern(topology = Seq(Vertex(Reference("v"), emptyObjPattern)))
    val expected =
      BindingContext(
        vertexBindings = Set(Reference("v")),
        edgeBindings = Set.empty,
        pathBindings = Set.empty)
    val actual = extractReferenceTuples(algebraTree)
    assert(actual == expected)
  }

  test("Vertex, edge and path references are correctly extracted") {
    val algebraTree =
      GraphPattern(
        topology = Seq(
          Vertex(Reference("v1"), emptyObjPattern),
          Vertex(Reference("v2"), emptyObjPattern),
          Edge(
            connName = Reference("e1"),
            leftEndpoint = Vertex(Reference("v1"), emptyObjPattern),
            rightEndpoint = Vertex(Reference("v3"), emptyObjPattern),
            connType = OutConn,
            expr = emptyObjPattern),
          Edge(
            connName = Reference("e2"),
            leftEndpoint = Vertex(Reference("v3"), emptyObjPattern),
            rightEndpoint = Vertex(Reference("v4"), emptyObjPattern),
            connType = OutConn,
            expr = emptyObjPattern),
          Path(
            connName = Reference("p1"),
            isReachableTest = true,
            leftEndpoint = Vertex(Reference("v5"), emptyObjPattern),
            rightEndpoint = Vertex(Reference("v6"), emptyObjPattern),
            connType = OutConn,
            expr = emptyObjPattern,
            quantifier = AllPaths, costVarDef = None,
            isObj = true,
            pathExpression = None),
          Path(
            connName = Reference("p2"),
            isReachableTest = true,
            leftEndpoint = Vertex(Reference("v1"), emptyObjPattern),
            rightEndpoint = Vertex(Reference("v2"), emptyObjPattern),
            connType = OutConn,
            expr = emptyObjPattern,
            quantifier = AllPaths, costVarDef = None,
            isObj = true,
            pathExpression = None)
        ))
    val expected =
      BindingContext(
        vertexBindings = Set(
          Reference("v1"), Reference("v2"), Reference("v3"), Reference("v4"),
          Reference("v5"), Reference("v6")),
        edgeBindings = Set(
          ReferenceTuple(Reference("e1"), Reference("v1"), Reference("v3")),
          ReferenceTuple(Reference("e2"), Reference("v3"), Reference("v4"))),
        pathBindings = Set(
          ReferenceTuple(Reference("p1"), Reference("v5"), Reference("v6")),
          ReferenceTuple(Reference("p2"), Reference("v1"), Reference("v2")))
      )
    val actual = extractReferenceTuples(algebraTree)
    assert(actual == expected)
  }
}
