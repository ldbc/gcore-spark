package spark.sql.operators

import algebra.expressions.AlgebraExpression
import planner.target_api.{BindingTable, PhysSelect}
import planner.trees.TargetTreeNode
import spark.sql.operators.SqlQueryGen._

case class SparkSelect(relation: TargetTreeNode, expr: AlgebraExpression)
  extends PhysSelect(relation, expr) {

  override val bindingTable: BindingTable = {
    val relationBtable: SparkBindingTable = relation.bindingTable.asInstanceOf[SparkBindingTable]

    val fromAlias: String = tempViewAlias

    val selectQuery: String =
      s"""
      SELECT * FROM (${relationBtable.btable.resQuery}) $fromAlias
      WHERE ${expressionToSelectionPred(expr, relationBtable.schemaMap, fromAlias)}"""

    relationBtable.copy(btableOps = SqlQuery(resQuery = selectQuery))
  }
}
