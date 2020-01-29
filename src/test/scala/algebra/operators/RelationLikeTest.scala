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

package algebra.operators

import algebra.expressions.{Label, Reference, True}
import algebra.types.AllPaths
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RelationLikeTest extends FunSuite {

  test("Empty BindingSet for Relation") {
    val relation = Relation(Label("foo"))
    assert(relation.getBindingSet.refSet.isEmpty)
  }

  test("Empty BindingSet for AllRelations") {
    val relation = AllRelations
    assert(relation.getBindingSet.refSet.isEmpty)
  }

  test("BindingSet = {ref} for VertexRelation") {
    val relation = VertexRelation(Reference("vref"), Relation(Label("vlabel")), True)
    assert(relation.getBindingSet.refSet == Set(Reference("vref")))
  }

  test("BindingSet = {fromRef, toRef, edgeRef} for EdgeRelation") {
    val from = VertexRelation(Reference("fromRef"), Relation(Label("fromLabel")), True)
    val to = VertexRelation(Reference("toRef"), Relation(Label("toLabel")), True)
    val edge =
      EdgeRelation(
        ref = Reference("edgeRef"),
        labelRelation = Relation(Label("edgeLabel")),
        expr = True,
        fromRel = from, toRel = to)
    assert(edge.getBindingSet.refSet ==
      Set(Reference("fromRef"), Reference("toRef"), Reference("edgeRef")))
  }

  test("BindingSet = {fromRef, toRef, pathRef} for StoredPathRelation") {
    val from = VertexRelation(Reference("fromRef"), Relation(Label("fromLabel")), True)
    val to = VertexRelation(Reference("toRef"), Relation(Label("toLabel")), True)
    val path =
      StoredPathRelation(
        ref = Reference("pathRef"),
        isReachableTest = true,
        labelRelation = Relation(Label("pathLabel")),
        expr = True,
        fromRel = from, toRel = to,
        costVarDef = None,
        quantifier = AllPaths)
    assert(path.getBindingSet.refSet ==
      Set(Reference("fromRef"), Reference("toRef"), Reference("pathRef")))
  }
}
