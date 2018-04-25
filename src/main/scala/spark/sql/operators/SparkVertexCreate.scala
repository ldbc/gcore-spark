package spark.sql.operators

import algebra.expressions.Reference
import planner.target_api.{BindingTable, PhysVertexCreate}
import planner.trees.TargetTreeNode

case class SparkVertexCreate(reference: Reference, relation: TargetTreeNode)
  extends PhysVertexCreate(reference, relation) {

  override val bindingTable: BindingTable = relation.bindingTable
}
