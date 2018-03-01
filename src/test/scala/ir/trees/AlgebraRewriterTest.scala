package ir.trees

import ir.algebra.expressions.{AlgebraExpression, Reference, True}
import ir.algebra.operators._
import ir.algebra.types.{DefaultGraph, GraphPattern, Vertex}
import org.scalatest.FunSuite

class AlgebraRewriterTest extends FunSuite {

  val expr: AlgebraExpression = True()
  val simpleMatch: AlgebraOperator =
    SimpleMatchClause(
      GraphPattern(Seq(Vertex(Reference("v"), expr))),
      DefaultGraph())
  val optionalMatch: AlgebraOperator =
    SimpleMatchClause(
      GraphPattern(Seq(Vertex(Reference("w"), expr))),
      DefaultGraph())

  test("CondMatchClause rewritten to FILTER(CART_PROD(matches), where)") {
    val tree = CondMatchClause(Seq(simpleMatch.asInstanceOf[SimpleMatchClause]), expr)
    val expected = Filter(CartesianProduct(Seq(simpleMatch)), expr)
    val actual = AlgebraRewriter.rewriteTree(tree)
    assert(actual == expected)
  }

  test("Match on non-optional and optional rewritten to LOJ(non-optional U optional)") {
    val condNonOptional = CondMatchClause(Seq(simpleMatch.asInstanceOf[SimpleMatchClause]), expr)
    val condOptional = CondMatchClause(Seq(optionalMatch.asInstanceOf[SimpleMatchClause]), expr)
    val tree = MatchClause(Seq(condNonOptional), Seq(condOptional))
    val expected =
      LeftOuterJoin(Seq(
        Filter(CartesianProduct(Seq(simpleMatch)), expr),
        Filter(CartesianProduct(Seq(optionalMatch)), expr)
      ))
    val actual = AlgebraRewriter.rewriteTree(tree)
    assert(actual == expected)
  }
}
