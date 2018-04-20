package planner.operators

import algebra.operators.{BasicConstructClause, MatchClause}
import algebra.trees.AlgebraTreeNode
import planner.trees.PlannerTreeNode

/**
  * The operation of building a new graph from a rewritten [[MatchClause]] clause and all the
  * rewritten [[BasicConstructClause]]s.
  */
case class CreateGraph(matchClause: AlgebraTreeNode,
                       constructClauses: Seq[PlannerTreeNode]) extends PlannerTreeNode {

  children = matchClause +: constructClauses
}
