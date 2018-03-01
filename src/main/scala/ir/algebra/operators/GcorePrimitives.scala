package ir.algebra.operators

import ir.algebra.expressions.AlgebraExpression
import ir.algebra.types.{Graph, GraphPattern}

abstract class GcorePrimitive extends AlgebraOperator

case class Query(matchClause: MatchClause) extends GcorePrimitive {
  children = List(matchClause)
}

abstract class MatchLike extends GcorePrimitive

// TODO: A SimpleMatchBlock also needs bindings, because the Match can happen in the presence of
// previously computed bindings.
case class SimpleMatchClause(graphPattern: GraphPattern, graph: Graph)
  extends MatchLike {

  children = List(graphPattern, graph)
}

case class CondMatchClause(simpleMatches: Seq[SimpleMatchClause], where: AlgebraExpression)
  extends MatchLike {

  children = simpleMatches :+ where
}

case class MatchClause(nonOptMatches: Seq[CondMatchClause], optMatches: Seq[CondMatchClause])
  extends MatchLike {

  children = nonOptMatches ++ optMatches
}
