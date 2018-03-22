package spark.sql.operators

import planner.trees.TargetTreeNode

case class SparkLeftOuterJoin(lhs: TargetTreeNode, rhs: TargetTreeNode)
  extends SparkJoin(lhs, rhs) {

  override def joinTypeSql: String = "LEFT OUTER JOIN"

  override def joinCondition: String = commonColumnsForJoin(lhsSchema, rhsSchema)
}
