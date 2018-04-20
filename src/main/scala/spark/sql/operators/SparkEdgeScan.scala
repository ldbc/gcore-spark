package spark.sql.operators

import algebra.expressions.{Label, Reference}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column.{fromIdColumn, idColumn, tableLabelColumn, toIdColumn}
import planner.operators.EdgeScan
import planner.target_api.{BindingTable, PhysEdgeScan}
import schema.Table
import spark.sql.operators.SqlQueryGen._

case class SparkEdgeScan(edgeScan: EdgeScan) extends PhysEdgeScan(edgeScan) {

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

    val addLabelFrom: String =
      s"""
      SELECT
      "$fromTableRef" AS `$fromRef$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(fromTable, fromBinding)}
      FROM global_temp.$fromTableRef"""

    val addLabelTo: String =
      s"""
      SELECT
      "$toTableRef" AS `$toRef$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(toTable, toBinding)}
      FROM global_temp.$toTableRef"""

    val addLabelEdge: String =
      s"""
      SELECT
      "$edgeTableRef" AS `$edgeRef$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(edgeTable, edgeBinding)}
      FROM global_temp.$edgeTableRef"""

    val joinEdgeOnFrom: String =
      s"""
      SELECT * FROM ($addLabelEdge) INNER JOIN ($addLabelFrom) ON
      `$edgeRef$$${fromIdColumn.columnName}` = `$fromRef$$${idColumn.columnName}`"""

    val joinEdgeOnFromAndTo: String =
      s"""
      SELECT * FROM ($joinEdgeOnFrom) INNER JOIN ($addLabelTo) ON
      `$edgeRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`"""

    SqlQuery(resQuery = joinEdgeOnFromAndTo)
  }

  private val newEdgeSchema: StructType = refactorScanSchema(edgeTable.data.schema, edgeBinding)
  private val newFromSchema: StructType = refactorScanSchema(fromTable.data.schema, fromBinding)
  private val newToSchema: StructType = refactorScanSchema(toTable.data.schema, toBinding)

  override val bindingTable: BindingTable =
    SparkBindingTable(
      sparkSchemaMap = Map(
        edgeBinding -> newEdgeSchema, fromBinding -> newFromSchema, toBinding -> newToSchema),
      sparkBtableSchema = mergeSchemas(newEdgeSchema, newFromSchema, newToSchema),
      btableOps = sqlQuery)
}
