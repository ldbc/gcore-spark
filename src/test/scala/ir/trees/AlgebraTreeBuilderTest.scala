package ir.trees

import ir.trees.algebra.SpoofaxToAlgebraMatch
import org.scalatest.FunSuite

class AlgebraTreeBuilderTest extends FunSuite with SpoofaxToAlgebraMatch {

  testsFor(build())
  testsFor(matchMixing())
  testsFor(graphPatterns())
  testsFor(expressions())
  testsFor(locations())
}
