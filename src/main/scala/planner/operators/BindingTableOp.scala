package planner.operators

import algebra.operators.RelationalOperator
import algebra.trees.AlgebraTreeNode
import planner.trees.PlannerTreeNode

/** A wrapper over a [[RelationalOperator]], in order to convert it to a [[PlannerTreeNode]]. */
case class BindingTableOp(op: AlgebraTreeNode) extends PlannerTreeNode {
  children = List(op)
}
