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

import algebra.expressions.Reference
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BindingContextTest extends FunSuite {

  test("allRefs") {
    val bindingContext =
      BindingContext(
        vertexBindings = Set(Reference("v0"), Reference("v1"), Reference("v2")),
        edgeBindings = Set(ReferenceTuple(Reference("e"), Reference("v1"), Reference("v2"))),
        pathBindings = Set(ReferenceTuple(Reference("p"), Reference("v1"), Reference("v2"))))
    val expected =
      Set(Reference("v0"), Reference("v1"), Reference("v2"), Reference("e"), Reference("p"))
    val actual = bindingContext.allRefs
    assert(actual == expected)
  }
}
