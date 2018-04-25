package planner.target_api

import algebra.expressions.Reference
import algebra.types.ConnectionType
import planner.trees.TargetTreeNode

abstract class PhysEdgeCreate(reference: Reference,
                              leftReference: Reference,
                              rightReference: Reference,
                              connType: ConnectionType,
                              relation: TargetTreeNode) extends TargetTreeNode {
  children = List(reference, leftReference, rightReference, connType, relation)
}
