package planner.operators

import algebra.trees.AlgebraTreeNode
import planner.trees.PlannerTreeNode

case class BindingTableOp(op: AlgebraTreeNode) extends PlannerTreeNode {
  children = List(op)
}
