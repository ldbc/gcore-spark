package spark.sql.operators

import planner.trees.TargetTreeNode

case class SparkInnerJoin(lhs: TargetTreeNode, rhs: TargetTreeNode) extends SparkJoin(lhs, rhs) {

  override def joinTypeSql: String = "INNER JOIN"

  override def joinCondition: String = commonColumnsForJoin(lhsSchema, rhsSchema)
}
