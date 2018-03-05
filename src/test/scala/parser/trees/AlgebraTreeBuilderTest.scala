package parser.trees

import org.scalatest.FunSuite

/**
  * Tests that the [[AlgebraTreeBuilder]] constructs a semantically correct algebraic tree from a
  * canonical parse tree. Does not test exception behavior, we are only interested in the tree
  * structure.
  */
class AlgebraTreeBuilderTest extends FunSuite with SpoofaxToAlgebraMatch {

  testsFor(build())
  testsFor(matchMixing())
  testsFor(graphPatterns())
  testsFor(expressions())
  testsFor(locations())
}
