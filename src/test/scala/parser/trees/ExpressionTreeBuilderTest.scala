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
      /* optional */ _)) =>

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

  def runTest(query: String, expected: AlgebraExpression): Unit = {
    val algebraTree = parse(query)

    inside (algebraTree) {
      case Query(
      _,
      MatchClause(
      /* non optional */ CondMatchClause(/*simpleMatchClauses =*/ _, /*where =*/ expr),
      /* optional */ _)) =>

        expr should matchPattern { case `expected` => }
    }
  }
}
