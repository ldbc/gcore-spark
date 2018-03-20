package spark.sql.operators

import algebra.expressions.{Label, Reference}
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column.tableLabelColumn
import planner.operators.{BindingTable, PhysVertexScan, VertexScan}
import planner.target_api.TargetPlanner
import planner.trees.PlannerContext
import schema.Table

case class SparkVertexScan(vertexScan: VertexScan,
                           graph: Graph,
                           targetPlanner: TargetPlanner,
                           plannerContext: PlannerContext)
  extends PhysVertexScan[SqlQuery](vertexScan, graph, plannerContext, targetPlanner)
    with SparkEntityScan {

  private val binding: Reference = vertexScan.binding
  private val tableName: Label = vertexScan.tableName
  private val physTable: Table[DataFrame] =
    physGraph.tableMap(tableName).asInstanceOf[Table[DataFrame]]

  private val sqlQuery: SqlQuery = {
    physTable.data.createOrReplaceGlobalTempView(tableName.value)
    val scanQuery: String =
      s"""
         | SELECT
         | "${tableName.value}" AS `${binding.refName}$$${tableLabelColumn.columnName}`,
         | ${renameColumnsQuery(physTable, binding)}
         | FROM global_temp.${tableName.value}
       """.stripMargin
    SqlQuery(prologue = Seq.empty, resQuery = scanQuery, epilogue = Seq.empty)
  }

  private val schema: StructType = refactorSchema(physTable.data.schema, binding)

  override val bindingTable: BindingTable =
    SparkBindingTable(schemas = Map(binding -> schema), btableOps = sqlQuery)
}
