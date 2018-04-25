package planner.operators

import algebra.expressions.Reference
import algebra.types.ConnectionType
import planner.trees.PlannerTreeNode

case class EdgeCreate(reference: Reference,
                      leftReference: Reference,
                      rightReference: Reference,
                      connType: ConnectionType,
                      bindingTable: PlannerTreeNode) extends PlannerTreeNode {

  children = List(reference, leftReference, rightReference, connType, bindingTable)
}
