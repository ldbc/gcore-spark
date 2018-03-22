package spark.sql.operators

import planner.trees.TargetTreeNode

case class SparkCrossJoin(lhs: TargetTreeNode, rhs: TargetTreeNode) extends SparkJoin(lhs, rhs) {

  override def joinTypeSql: String = "CROSS JOIN"

  override def joinCondition: String = ""
}
