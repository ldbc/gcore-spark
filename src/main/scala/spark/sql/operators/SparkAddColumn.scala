package spark.sql.operators

import algebra.expressions.Reference
import planner.target_api.{BindingTable, PhysAddColumn}
import planner.trees.TargetTreeNode

case class SparkAddColumn(reference: Reference, relation: TargetTreeNode)
  extends PhysAddColumn(reference, relation) {

  override val bindingTable: BindingTable = relation.bindingTable
}
