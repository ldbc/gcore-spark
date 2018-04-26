package algebra.operators

import algebra.trees.AlgebraTreeNode
import common.compiler.Context

/**
  * The operation of building a new graph from a rewritten [[MatchClause]] clause and all the
  * rewritten [[BasicConstructClause]]s.
  */
case class GraphCreate(matchClause: AlgebraTreeNode,
                       constructClauses: Seq[AlgebraTreeNode]) extends GcoreOperator {

  children = matchClause +: constructClauses

  override def checkWithContext(context: Context): Unit = {}
}
