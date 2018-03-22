package planner.target_api

import planner.trees.TargetTreeNode

abstract class PhysJoin(lhs: TargetTreeNode, rhs: TargetTreeNode) extends TargetTreeNode {

  children = List(lhs, rhs)
}
