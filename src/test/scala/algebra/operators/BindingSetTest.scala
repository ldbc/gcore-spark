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
class BindingSetTest extends FunSuite {

  test("++ with other table") {
    val btable1 = new BindingSet(Reference("foo"), Reference("bar"))
    val btable2 = new BindingSet(Reference("baz"))
    val btable3 = new BindingSet(Reference("foo"), Reference("qux"))

    assert(btable1 ++ btable2 ===
      new BindingSet(Reference("foo"), Reference("bar"), Reference("baz")))
    assert(btable1 ++ btable3 ===
      new BindingSet(Reference("foo"), Reference("bar"), Reference("qux")))
  }

  test("intersect sequence of BindingSets") {
    val bset1 = Set(Reference("foo"), Reference("bar"))
    val bset2 = Set(Reference("foo"), Reference("qux"))
    val bset3 = Set(Reference("foo"), Reference("qux"), Reference("quux"))

    assert(
      BindingSet.intersectBindingSets(Seq(bset1, bset2, bset3)) ==
      Set(Reference("foo"), Reference("qux"))
    )
  }
}
