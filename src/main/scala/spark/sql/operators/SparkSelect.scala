package spark.sql.operators

import algebra.expressions.AlgebraExpression
import planner.operators.BindingTable
import planner.target_api.PhysSelect
import planner.trees.TargetTreeNode

case class SparkSelect(relation: TargetTreeNode, expr: AlgebraExpression)
  extends PhysSelect(relation, expr) with SqlQueryGen {

  override val bindingTable: BindingTable = {
    val relationBtable: SparkBindingTable = relation.bindingTable.asInstanceOf[SparkBindingTable]
    val relationTempView: String = tempViewNameWithUID("relation_temp")

    val createRelationTempView: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$relationTempView` AS ${relationBtable.btable.resQuery}
       """.stripMargin

    val selectQuery: String =
      s"""
         | SELECT * FROM `$relationTempView` WHERE ${expressionToSelectionPred(expr)}
       """.stripMargin

    val dropRelationTempView = s"DROP VIEW `$relationTempView`"

    relationBtable.copy(
      btableOps =
        SqlQuery(
          prologue = relationBtable.btableOps.prologue :+ createRelationTempView,
          resQuery = selectQuery,
          epilogue = relationBtable.btableOps.epilogue :+ dropRelationTempView)
    )
  }
}
