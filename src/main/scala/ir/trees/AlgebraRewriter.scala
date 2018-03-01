package ir.trees

import ir.algebra.operators._

/**
  * Transforms the [[GcorePrimitive]]s within the algebraic tree into a mix of common
  * [[AlgebraPrimitive]]s.
  *
  * Applied transformation rules:
  *
  * simpleMatches @ Seq[SimpleMatch] => CartesianProduct(simpleMatches)
  * condMatch @ CondMatchClause(simpleMatches, expr) => Filter(simpleMatches, expr)
  * match @ MatchClause(nonOptMatches, optMatches) => LeftOuterJoin(nonOptMatches ++ optMatches)
  *
  * TODO: The current list of transformations is incomplete, add the rest of the transformations in
  * this rewriter.
  *
  * TODO: These transformation rules depend on the order of the children. Can we simplify the rules?
  */
object AlgebraRewriter extends BottomUpRewriter[AlgebraTreeNode] {

  override def rule: RewriteFuncType =
    lojMatchesWithOptionalMatches orElse filterCondMatches

  private val lojMatchesWithOptionalMatches: RewriteFuncType = {
    case m @ MatchClause(_, _) => LeftOuterJoin(m.children.map(_.asInstanceOf[AlgebraOperator]))
  }

  private val filterCondMatches: RewriteFuncType = {
    case m @ CondMatchClause(simpleMatches, where) =>
      Filter(CartesianProduct(m.children.init.map(_.asInstanceOf[AlgebraOperator])), where)
  }
}
