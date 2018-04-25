package planner.target_api

import algebra.expressions.Reference
import planner.trees.TargetTreeNode

abstract class PhysAddColumn(reference: Reference,
                             relation: TargetTreeNode) extends TargetTreeNode {

  children = List(reference, relation)
}
