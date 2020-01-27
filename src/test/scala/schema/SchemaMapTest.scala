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

package schema

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Test with ignore must be tested manually first and need closer inspection
  */
@RunWith(classOf[JUnitRunner])
class SchemaMapTest extends FunSuite {

  val map1 = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3))
  val map2 = SchemaMap(Map("qux" -> 1, "fred" -> 2))
  val emptyMap = SchemaMap.empty

  test("Union with disjoint map creates a map with key-value pairs from both sides of union") {
    val expectedUnion = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3, "qux" -> 1, "fred" -> 2))
    assert((map1 union map2) == expectedUnion)
    assert((map2 union map1) == expectedUnion)
  }

  ignore("Union with non-disjoint map throws exception") {
    assertThrows[SchemaException] {
      map1 union map1
    }
  }

  test("get") {
    assert(map1.get("foo").isDefined)
    assert(map1.get("foo").get == 1)
    assert(map1.get("qux").isEmpty)
  }

  test("keys") {
    assert(map1.keys == Seq("foo", "bar", "baz"))
    assert(emptyMap.keys == Seq.empty)
  }

  test("values") {
    assert(map1.values == Seq(1, 2, 3))
    assert(emptyMap.values == Seq.empty)
  }
}
