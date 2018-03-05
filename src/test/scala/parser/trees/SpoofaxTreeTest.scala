package parser.trees

import org.scalatest.FunSuite

/** Tests for the [[SpoofaxTreeBuilder]] and [[SpoofaxCanonicalRewriter]]. */
class SpoofaxTreeTest extends FunSuite
  with BuildMatchClause
  with RewriteMatchClause {

  testsFor(buildMatchSubtree())
  testsFor(bindUnnamedVariables())
}
