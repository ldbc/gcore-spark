package parser.trees

import algebra.operators.Query
import org.scalatest.{FunSuite, Matchers}

class AlgebraTreeBuilderTest extends FunSuite with Matchers with MinimalSpoofaxParser {

  test("Builds a Query node") {
    val algebraTree = parse("CONSTRUCT (v) MATCH (v)")
    algebraTree should matchPattern { case _: Query => }
  }
}
