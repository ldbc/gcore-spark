package spark.sql.operators

import algebra.target_api.TargetTreeNode
import spark.sql.SqlQuery.commonColumnsForJoin

case class LeftOuterJoin(lhs: TargetTreeNode, rhs: TargetTreeNode) extends Join(lhs, rhs) {

  override def joinTypeSql: String = "LEFT OUTER JOIN"

  override def joinCondition: String = commonColumnsForJoin(lhsSchema, rhsSchema)
}
