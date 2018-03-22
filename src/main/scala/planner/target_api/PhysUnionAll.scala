package planner.target_api

import planner.trees.TargetTreeNode

abstract class PhysUnionAll(lhs: TargetTreeNode, rhs: TargetTreeNode) extends TargetTreeNode {

  children = List(lhs, rhs)
}
