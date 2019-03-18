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

package parser.trees

import algebra.expressions._
import algebra.operators.{CondMatchClause, MatchClause, Query}
import algebra.types.GraphPattern
import org.scalatest.{FunSuite, Inside, Matchers}

class ExpressionTreeBuilderTest extends FunSuite
  with Matchers with Inside with MinimalSpoofaxParser {

  test("WHERE BasicGraphPattern => WHERE EXISTS (CONSTRUCT (u) MATCH BasicGraphPattern") {
    val algebraTree = parse("CONSTRUCT (u) MATCH (u) WHERE (u:Label)")

    inside (algebraTree) {
      case Query(
      _,
      MatchClause(
      /* non optional */ CondMatchClause(/*simpleMatchClauses =*/ _, /*where =*/ expr),
      /* optional */ _,True)) =>

        expr should matchPattern { case Exists(GraphPattern(_)) => }
    }
  }

  test("WHERE NOT 2 => Not(2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE NOT 2"
    val expected = Not(IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE -2 => Minus(2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE -2"
    val expected = Minus(IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 AND 2 => And(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 AND 2"
    val expected = And(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 OR 2 => Or(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 OR 2"
    val expected = Or(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 = 2 => Eq(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 = 2"
    val expected = Eq(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 != 2 (Neq1) => Neq(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 != 2"
    val expected = Neq(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 <> 2 (Neq2) => Neq(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 <> 2"
    val expected = Neq(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 > 2 => Gt(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 > 2"
    val expected = Gt(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 >= 2 => Gte(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 >= 2"
    val expected = Gte(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 < 2 => Lt(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 < 2"
    val expected = Lt(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 <= 2 => Lte(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 <= 2"
    val expected = Lte(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2^2 => Power(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2^2"
    val expected = Power(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2*2 => Mul(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2*2"
    val expected = Mul(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2/2 => Div(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2/2"
    val expected = Div(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2%2 => Mod(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2%2"
    val expected = Mod(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2+2 => Add(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2+2"
    val expected = Add(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2-2 => Sub(2, 2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2-2"
    val expected = Sub(lhs = IntLiteral(2), rhs = IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 IS NULL => IsNull(2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 IS NULL"
    val expected = IsNull(IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE 2 IS NOT NULL => IsNotNull(2)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE 2 IS NOT NULL"
    val expected = IsNotNull(IntLiteral(2))
    runTest(query, expected)
  }

  test("WHERE u.prop => PropRef(u, prop)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE u.prop"
    val expected = PropertyRef(Reference("u"), PropertyKey("prop"))
    runTest(query, expected)
  }

  test("WHERE count(*) => Count(Star)") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE count(*)"
    val expected = Count(distinct = false, Star)
    runTest(query, expected)
  }

  test("WHERE count(distinct *) => Count(distinct = true, Star") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE count(distinct *)"
    val expected = Count(distinct = true, Star)
    runTest(query, expected)
  }

  test("WHERE collect(u.prop) => Collect(PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE collect(u.prop)"
    val expected = Collect(distinct = false, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE collect(distinct u.prop) => Collect(distinct = true, PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE collect(distinct u.prop)"
    val expected = Collect(distinct = true, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE min(u.prop) => Min(PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE min(u.prop)"
    val expected = Min(distinct = false, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE min(distinct u.prop) => Min(distinct = true, PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE min(distinct u.prop)"
    val expected = Min(distinct = true, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE max(u.prop) => Max(PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE max(u.prop)"
    val expected = Max(distinct = false, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE max(distinct u.prop) => Max(distinct = true, PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE max(distinct u.prop)"
    val expected = Max(distinct = true, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE avg(u.prop) => Avg(PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE avg(u.prop)"
    val expected = Avg(distinct = false, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE avg(distinct u.prop) => Avg(distinct = true, PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE avg(distinct u.prop)"
    val expected = Avg(distinct = true, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE sum(u.prop) => Sum(PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE sum(u.prop)"
    val expected = Sum(distinct = false, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE sum(distinct u.prop) => Sum(distinct = true, PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE sum(distinct u.prop)"
    val expected = Sum(distinct = true, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE group_concat(u.prop) => GroupConcat(PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE group_concat(u.prop)"
    val expected = GroupConcat(distinct = false, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  test("WHERE group_concat(distinct u.prop) => GroupConcat(distinct = true, PropRef(u, prop))") {
    val query = "CONSTRUCT (u) MATCH (u) WHERE group_concat(distinct u.prop)"
    val expected = GroupConcat(distinct = true, PropertyRef(Reference("u"), PropertyKey("prop")))
    runTest(query, expected)
  }

  def runTest(query: String, expected: AlgebraExpression): Unit = {
    val algebraTree = parse(query)

    inside (algebraTree) {
      case Query(
      _,
      MatchClause(
      /* non optional */ CondMatchClause(/*simpleMatchClauses =*/ _, /*where =*/ expr),
      /* optional */ _,True)) =>

        expr should matchPattern { case `expected` => }
    }
  }
}
