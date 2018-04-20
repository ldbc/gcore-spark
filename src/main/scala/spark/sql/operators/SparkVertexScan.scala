package spark.sql.operators

import algebra.expressions.{Label, Reference}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column.tableLabelColumn
import planner.operators.VertexScan
import planner.target_api.{BindingTable, PhysVertexScan}
import schema.Table
import spark.sql.operators.SqlQueryGen._

case class SparkVertexScan(vertexScan: VertexScan) extends PhysVertexScan(vertexScan) {

  private val binding: Reference = vertexScan.binding
  private val tableName: Label = vertexScan.tableName
  private val physTable: Table[DataFrame] =
    physGraph.tableMap(tableName).asInstanceOf[Table[DataFrame]]

  private val sqlQuery: SqlQuery = {
    physTable.data.createOrReplaceGlobalTempView(tableName.value)
    val scanQuery: String =
      s"""
      SELECT
      "${tableName.value}" AS `${binding.refName}$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(physTable, binding)}
      FROM global_temp.${tableName.value}"""
    SqlQuery(resQuery = scanQuery)
  }

  private val schema: StructType = refactorScanSchema(physTable.data.schema, binding)

  override val bindingTable: BindingTable =
    SparkBindingTable(
      sparkSchemaMap = Map(binding -> schema),
      sparkBtableSchema = schema,
      btableOps = sqlQuery)
}
