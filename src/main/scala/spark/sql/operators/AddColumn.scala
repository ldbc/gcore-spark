package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.TargetTreeNode

case class AddColumn(reference: Reference, relation: TargetTreeNode)
  extends target_api.AddColumn(reference, relation) {

  override val bindingTable: target_api.BindingTableMetadata = relation.bindingTable
}
