package ir.algebra

import ir.algebra.expressions.AlgebraExpression
import ir.algebra.types.{Graph, GraphPattern}
import ir.trees.AlgebraTreeNode

abstract class AlgebraOperator extends AlgebraTreeNode

case class Query(matchClause: MatchClause) extends AlgebraOperator {
  children = List(matchClause)
}

// TODO: A SimpleMatchBlock also needs bindings, because the Match can happen in the presence of
// previously computed bindings.
case class SimpleMatchClause(graphPattern: GraphPattern, graph: Graph, isOptional: Boolean)
  extends AlgebraOperator {

  children = List(graphPattern, graph)

  override def toString: String = s"${name} [isOptional=${isOptional}]"
}

case class CondMatchClause(simpleMatches: Seq[SimpleMatchClause], where: AlgebraExpression)
  extends AlgebraOperator {

  children = simpleMatches :+ where
}

case class MatchClause(condMatches: Seq[CondMatchClause]) extends AlgebraOperator {
  children = condMatches
}
