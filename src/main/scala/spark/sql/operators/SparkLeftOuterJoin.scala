package spark.sql.operators

import planner.target_api.TargetPlanner
import planner.trees.TargetTreeNode

case class SparkLeftOuterJoin(lhs: TargetTreeNode,
                              rhs: TargetTreeNode,
                              targetPlanner: TargetPlanner)
  extends SparkJoin(lhs, rhs, targetPlanner) {

  override def joinTypeSql: String = "LEFT OUTER JOIN"

  override def joinCondition: String = commonColumnsForJoin(lhsSchema, rhsSchema)
}
