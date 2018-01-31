package ir

import org.scalatest.FunSuite
import utils.Gcore

class SpoofaxToIrRewriterTest extends FunSuite
  with Match
  with MatchBasicGraphPattern {

  val gcore: Gcore = new Gcore
  val rewriter: SpoofaxToIrRewriter = new SpoofaxToIrRewriter

  /** MATCH tests. */
  testsFor(matchPatternMix(gcore, rewriter))
  testsFor(rewriteUnnamedVariables(gcore, rewriter))
  testsFor(objectMatchPattern(gcore, rewriter))
  testsFor(basicGraphPattern(gcore, rewriter))
}
