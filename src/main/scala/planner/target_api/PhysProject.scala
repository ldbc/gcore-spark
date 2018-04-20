package planner.target_api

import algebra.expressions.Reference
import planner.trees.TargetTreeNode

abstract class PhysProject(relation: TargetTreeNode, attributes: Seq[Reference])
  extends TargetTreeNode {

  children = relation +: attributes
}
