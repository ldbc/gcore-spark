package planner.operators

import algebra.expressions.Reference
import planner.trees.PlannerTreeNode

case class VertexCreate(reference: Reference,
                        bindingTable: PlannerTreeNode) extends PlannerTreeNode {

  children = List(reference, bindingTable)
}
