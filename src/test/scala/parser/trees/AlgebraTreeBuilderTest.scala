package parser.trees

import org.scalatest.FunSuite
import parser.trees.algebra.SpoofaxToAlgebraMatch

class AlgebraTreeBuilderTest extends FunSuite with SpoofaxToAlgebraMatch {

  testsFor(build())
  testsFor(matchMixing())
  testsFor(graphPatterns())
  testsFor(expressions())
  testsFor(locations())
}
