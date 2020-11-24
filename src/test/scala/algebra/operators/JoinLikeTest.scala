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

import algebra.expressions.Reference
import algebra.operators.BinaryOperator.reduceLeft
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JoinLikeTest extends FunSuite {

  /**       rel1 | rel2 | rel3 | rel4 | rel5
    *      +-----+------+------+------+-----+
    * a => |  y  |      |      |      |     +
    *      +-----+------+------+------+-----+
    * b => |  y  |   y  |      |      |  y  +
    *      +-----+------+------+------+-----+
    * c => |     |   y  |      |      |     +
    *      +-----+------+------+------+-----+
    * d => |     |   y  |      |  y   |     +
    *      +-----+------+------+------+-----+
    * e => |     |      |   y  |      |     +
    *      +-----+------+------+------+-----+
    * f => |     |      |   y  |      |     +
    *      +-----+------+------+------+-----+
    * g => |     |      |      |  y   |  y  +
    *      +-----+------+------+------+-----+
    */
  val rel1 = SimpleRel(Reference("a"), Reference("b"))
  val rel2 = SimpleRel(Reference("b"), Reference("c"), Reference("d"))
  val rel3 = SimpleRel(Reference("e"), Reference("f"))
  val rel4 = SimpleRel(Reference("d"), Reference("g"))
  val rel5 = SimpleRel(Reference("b"), Reference("g"))

  test("Common bindings of two joined relations") {
    val joined = AJoin(rel1, rel2)
    assert(joined.commonInSeenBindingSets == Set(Reference("b")))
  }

  test("Common bindings of reduced relations") {
    val joined = reduceLeft(Seq(rel1, rel2, rel3, rel4, rel5), AJoin).asInstanceOf[AJoin]
    assert(
      joined.commonInSeenBindingSets ==
        Set(Reference("b"), Reference("d"), Reference("g")))
  }

  sealed case class AJoin(lhs: RelationLike,
                          rhs: RelationLike,
                          bindingTable: Option[BindingSet] = None)
    extends JoinLike(lhs, rhs, bindingTable)

  sealed case class SimpleRel(refs: Reference*) extends RelationLike(new BindingSet(refs: _*))
}
