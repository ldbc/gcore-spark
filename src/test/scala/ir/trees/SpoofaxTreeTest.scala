package ir.trees

import ir.trees.spoofax.{BuildMatchClause, RewriteMatchClause}
import org.scalatest.FunSuite

/** Tests for the [[SpoofaxTreeBuilder]] and [[ir.rewriters.SpoofaxCanonicalRewriter]]. */
class SpoofaxTreeTest extends FunSuite
  with BuildMatchClause
  with RewriteMatchClause {

  testsFor(buildMatchSubtree())
  testsFor(bindUnnamedVariables())
}
