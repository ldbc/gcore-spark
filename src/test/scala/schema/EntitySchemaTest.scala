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

import algebra.expressions.{Label, PropertyKey}
import algebra.types.{GcoreInteger, GcoreString}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EntitySchemaTest extends FunSuite {

  val schema1 = EntitySchema(SchemaMap(Map(
    Label("person") -> SchemaMap(Map(
      PropertyKey("age") -> GcoreInteger,
      PropertyKey("address") -> GcoreString)),
    Label("city") -> SchemaMap(Map(
      PropertyKey("population") -> GcoreInteger))
  )))

  val schema2 = EntitySchema(SchemaMap(Map(
    Label("car") -> SchemaMap(Map(
      PropertyKey("type") -> GcoreString,
      PropertyKey("manufacturer") -> GcoreString))
  )))

  val schema1UnionSchema2 = EntitySchema(SchemaMap(Map(
    Label("person") -> SchemaMap(Map(
      PropertyKey("age") -> GcoreInteger,
      PropertyKey("address") -> GcoreString)),
    Label("city") -> SchemaMap(Map(
      PropertyKey("population") -> GcoreInteger)),
    Label("car") -> SchemaMap(Map(
      PropertyKey("type") -> GcoreString,
      PropertyKey("manufacturer") -> GcoreString))
  )))

  val emptySchema = EntitySchema.empty


  test("Union with other schema creates a schema with key-value pairs from both sides of union") {
    assert((schema1 union schema2) == schema1UnionSchema2)
  }

  test("Union with empty schema is idempotent") {
    assert((schema1 union emptySchema) == schema1)
  }

  test("Union is commutative") {
    assert((schema1 union schema2) == schema1UnionSchema2)
    assert((schema2 union schema1) == schema1UnionSchema2)
  }

  test("labels") {
    assert(schema1.labels == Seq(Label("person"), Label("city")))
  }

  test("properties(label)") {
    assert(schema1.properties(Label("person")) == Seq(PropertyKey("age"), PropertyKey("address")))
    assert(schema1.properties(Label("car")) == Seq.empty)
  }

  test("properties") {
    assert(
      schema1.properties ==
        Seq(PropertyKey("age"), PropertyKey("address"), PropertyKey("population")))
  }
}
