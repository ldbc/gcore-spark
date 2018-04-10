package parser.trees

import algebra.operators.{ConstructClause, MatchClause, Query}
import org.scalatest.{FunSuite, Matchers}

class QueryTreeBuilderTest extends FunSuite with Matchers with MinimalSpoofaxParser {

  test("Query node has children: ConstructClause, MatchClause") {
    val algebraTree = parse("CONSTRUCT (v) MATCH (v)")
    algebraTree should matchPattern {
      case Query(_: ConstructClause, _: MatchClause) =>
    }
  }
}
