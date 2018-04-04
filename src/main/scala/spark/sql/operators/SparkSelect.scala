package spark.sql.operators

import algebra.expressions.AlgebraExpression
import planner.operators.BindingTable
import planner.target_api.PhysSelect
import planner.trees.TargetTreeNode

case class SparkSelect(relation: TargetTreeNode, expr: AlgebraExpression)
  extends PhysSelect(relation, expr) with SqlQueryGen {

  override val bindingTable: BindingTable = {
    val relationBtable: SparkBindingTable = relation.bindingTable.asInstanceOf[SparkBindingTable]

    val fromAlias: String = tempViewAlias

    val selectQuery: String =
      s"""
         | SELECT * FROM (${relationBtable.btable.resQuery}) $fromAlias
         | WHERE ${expressionToSelectionPred(expr, relationBtable.schemas, fromAlias)}
       """.stripMargin

    relationBtable.copy(btableOps = SqlQuery(resQuery = selectQuery))
  }
}
