package planner.target_api

import algebra.expressions.Reference
import planner.trees.TargetTreeNode

abstract class PhysVertexCreate(reference: Reference,
                                relation: TargetTreeNode) extends TargetTreeNode {
  children = List(reference, relation)
}