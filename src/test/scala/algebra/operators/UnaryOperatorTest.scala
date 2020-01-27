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
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UnaryOperatorTest extends FunSuite {

  test("BindingSet of RelationLike becomes BindingSet of UnaryOperator") {
    val bset = new BindingSet(Reference("ref1"), Reference("ref2"))
    val op = AUnaryOperator(ARelationLike(bset), bindingSet = None)

    assert(op.getBindingSet.refSet == bset.refSet)
  }

  test("BindingSet of RelationLike is overriden if explicitly provided") {
    val bset = new BindingSet(Reference("ref1"), Reference("ref2"))
    val overrideBset = new BindingSet(Reference("ref3"))
    val op = AUnaryOperator(ARelationLike(bset), bindingSet = Some(overrideBset))

    assert(op.getBindingSet.refSet == overrideBset.refSet)
  }

  sealed case class AUnaryOperator(relation: RelationLike, bindingSet: Option[BindingSet] = None)
    extends UnaryOperator(relation, bindingSet)

  sealed case class ARelationLike(bindingSet: BindingSet) extends RelationLike(bindingSet)
}
