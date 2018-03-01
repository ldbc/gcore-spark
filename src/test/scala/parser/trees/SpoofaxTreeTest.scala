package parser.trees

import org.scalatest.FunSuite
import parser.trees.spoofax.{BuildMatchClause, RewriteMatchClause}

/** Tests for the [[SpoofaxTreeBuilder]] and [[SpoofaxCanonicalRewriter]]. */
class SpoofaxTreeTest extends FunSuite
  with BuildMatchClause
  with RewriteMatchClause {

  testsFor(buildMatchSubtree())
  testsFor(bindUnnamedVariables())
}
