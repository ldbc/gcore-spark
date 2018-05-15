package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.TargetTreeNode

/**
  * Adds a new column to a table. Since the implementation of this operator is resolved through the
  * [[Project]] operator, we simply pass on the [[target_api.BindingTableMetadata]] of the received
  * [[relation]] to the parent node.
  */
case class AddColumn(reference: Reference, relation: TargetTreeNode)
  extends target_api.AddColumn(reference, relation) {

  override val bindingTable: target_api.BindingTableMetadata = relation.bindingTable
}
