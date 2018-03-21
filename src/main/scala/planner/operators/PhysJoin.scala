package planner.operators

import planner.target_api.TargetPlanner
import planner.trees.TargetTreeNode

abstract class PhysJoin(lhs: TargetTreeNode, rhs: TargetTreeNode, targetPlanner: TargetPlanner)
  extends TargetTreeNode(targetPlanner) {

  children = List(lhs, rhs)
}
