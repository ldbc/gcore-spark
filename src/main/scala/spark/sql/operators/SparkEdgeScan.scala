package spark.sql.operators

import algebra.expressions.{Label, Reference}
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import planner.operators.Column.{fromIdColumn, idColumn, tableLabelColumn, toIdColumn}
import planner.operators.{BindingTable, EdgeScan, PhysEdgeScan}
import planner.target_api.TargetPlanner
import planner.trees.PlannerContext
import schema.Table

case class SparkEdgeScan(edgeScan: EdgeScan,
                         graph: Graph,
                         plannerContext: PlannerContext,
                         targetPlanner: TargetPlanner)
  extends PhysEdgeScan[SqlQuery](edgeScan, graph, plannerContext, targetPlanner)
    with SparkEntityScan {

  private val edgeBinding: Reference = edgeScan.edgeBinding
  private val fromBinding: Reference = edgeScan.fromBinding
  private val toBinding: Reference = edgeScan.toBinding

  private val edgeTableName: Label = edgeScan.edgeTableName
  private val fromTableName: Label = edgeScan.fromTableName
  private val toTableName: Label = edgeScan.toTableName

  private val edgeTable: Table[DataFrame] =
    physGraph.tableMap(edgeTableName).asInstanceOf[Table[DataFrame]]
  private val fromTable: Table[DataFrame] =
    physGraph.tableMap(fromTableName).asInstanceOf[Table[DataFrame]]
  private val toTable: Table[DataFrame] =
    physGraph.tableMap(toTableName).asInstanceOf[Table[DataFrame]]

  private val sqlQuery: SqlQuery = {
    edgeTable.data.createOrReplaceGlobalTempView(edgeTableName.value)
    fromTable.data.createOrReplaceGlobalTempView(fromTableName.value)
    toTable.data.createOrReplaceGlobalTempView(toTableName.value)

    val edgeRef: String = edgeBinding.refName
    val fromRef: String = fromBinding.refName
    val toRef: String = toBinding.refName

    val edgeTableRef: String = edgeTableName.value
    val fromTableRef: String = fromTableName.value
    val toTableRef: String = toTableName.value

    val joinOnFromTempView: String = s"${edgeRef}_temp_edgeJoinedOnFrom"
    val fromTempView: String = s"${fromRef}_temp_fromWithLabel"
    val toTempView: String = s"${toRef}_temp_toWithLabel"
    val edgeTempView: String = s"${edgeRef}_temp_edgeWithLabel"

    val addLabelFrom: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW $fromTempView AS
         | SELECT
         | "$fromTableRef" AS `$fromRef$$${tableLabelColumn.columnName}`,
         | ${renameColumnsQuery(fromTable, fromBinding)}
         | FROM global_temp.$fromTableRef
       """.stripMargin

    val addLabelTo: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW $toTempView AS
         | SELECT
         | "$toTableRef" AS `$toRef$$${tableLabelColumn.columnName}`,
         | ${renameColumnsQuery(toTable, toBinding)}
         | FROM global_temp.$toTableRef
       """.stripMargin

    val addLabelEdge: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW $edgeTempView AS
         | SELECT
         | "$edgeTableRef" AS `$edgeRef$$${tableLabelColumn.columnName}`,
         | ${renameColumnsQuery(edgeTable, edgeBinding)}
         | FROM global_temp.$edgeTableRef
       """.stripMargin

    val joinEdgeOnFrom: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW $joinOnFromTempView AS
         | SELECT * FROM $edgeTempView
         | FULL OUTER JOIN $fromTempView ON
         | `$edgeRef$$${fromIdColumn.columnName}` = `$fromRef$$${idColumn.columnName}`
       """.stripMargin

    val joinEdgeOnFromAndTo: String =
      s"""
         | SELECT * FROM $joinOnFromTempView FULL OUTER JOIN $toTempView ON
         | `$edgeRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`
       """.stripMargin

    val cleanupJoinOnFromTempView: String = s"DROP VIEW $joinOnFromTempView"
    val cleanupFromTempView: String = s"DROP VIEW $fromTempView"
    val cleanupToTempView: String = s"DROP VIEW $toTempView"
    val cleanupEdgeTempView: String = s"DROP VIEW $edgeTempView"

    SqlQuery(
      prologue = Seq(addLabelFrom, addLabelTo, addLabelEdge, joinEdgeOnFrom),
      resQuery = joinEdgeOnFromAndTo,
      epilogue =
        Seq(cleanupFromTempView, cleanupToTempView, cleanupEdgeTempView, cleanupJoinOnFromTempView))
  }

  override val bindingTable: BindingTable =
    SparkBindingTable(
      schemas = Map(
        edgeBinding -> refactorSchema(edgeTable.data.schema, edgeBinding),
        fromBinding -> refactorSchema(fromTable.data.schema, fromBinding),
        toBinding -> refactorSchema(toTable.data.schema, toBinding)),
      btableOps = sqlQuery
    )
}
