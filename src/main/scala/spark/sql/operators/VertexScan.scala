package spark.sql.operators

import algebra.operators.Column._
import algebra.operators.VertexRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import schema.{GraphDb, Table}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

case class VertexScan(vertexRelation: VertexRelation, graph: Graph, graphDb: GraphDb)
  extends target_api.VertexScan(vertexRelation, graph, graphDb) {

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

  override val bindingTable: BindingTableMetadata =
    SqlBindingTableMetadata(
      sparkSchemaMap = Map(binding -> schema),
      sparkBtableSchema = schema,
      btableOps = sqlQuery)
}
