package spark.sql.operators

import algebra.target_api.TargetTreeNode
import spark.sql.SqlQuery.commonColumnsForJoin

case class InnerJoin(lhs: TargetTreeNode, rhs: TargetTreeNode) extends Join(lhs, rhs) {

  override def joinTypeSql: String = "INNER JOIN"

  override def joinCondition: String = commonColumnsForJoin(lhsSchema, rhsSchema)
}
