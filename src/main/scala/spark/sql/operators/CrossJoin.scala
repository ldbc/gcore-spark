package spark.sql.operators

import algebra.target_api.TargetTreeNode

case class CrossJoin(lhs: TargetTreeNode, rhs: TargetTreeNode) extends Join(lhs, rhs) {

  override def joinTypeSql: String = "CROSS JOIN"

  override def joinCondition: String = ""
}
