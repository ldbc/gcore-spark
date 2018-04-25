package spark.sql.operators

import algebra.expressions.Reference
import algebra.types.ConnectionType
import planner.target_api.{BindingTable, PhysEdgeCreate}
import planner.trees.TargetTreeNode

case class SparkEdgeCreate(reference: Reference,
                           leftReference: Reference,
                           rightReference: Reference,
                           connType: ConnectionType,
                           relation: TargetTreeNode)
  extends PhysEdgeCreate(reference, leftReference, rightReference, connType, relation) {

  override val bindingTable: BindingTable = relation.bindingTable
}
