package algebra.operators

import algebra.trees.AlgebraTreeNode
import common.compiler.Context

/**
  * The operation of building a new graph from a rewritten [[MatchClause]] clause and all the
  * rewritten [[CondConstructClause]]s.
  */
case class GraphCreate(matchClause: AlgebraTreeNode,
                       groupConstructs: Seq[AlgebraTreeNode]) extends GcoreOperator {

  children = matchClause +: groupConstructs

  override def checkWithContext(context: Context): Unit = {}
}
