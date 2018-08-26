package spark.sql.operators

import algebra.operators.Column._
import algebra.operators.EdgeRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import schema.{Catalog, Table}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

/**
  * Creates the table that will hold information about an edge and its endpoints.
  *
  * Each edge and vertex type (denoted by the entity label) is stored into a separate table in the
  * database. For example, for the edge (a)-[e]->(b), we will need three tables, a's, e's and b's,
  * to create the result of the [[EdgeScan]] operation. To do this, we first create temporary views
  * over a's, b's and e's tables. We then join e's table with a's on e.fromid == a.id and the result
  * with b's table on e.toid == b.id.
  */
case class EdgeScan(edgeRelation: EdgeRelation, graph: Graph, catalog: Catalog)
  extends target_api.EdgeScan(edgeRelation, graph, catalog) {

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
      "$fromTableRef" AS `$fromRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(fromTable.data, fromBinding)}
      FROM global_temp.$fromTableRef"""

    val addLabelTo: String =
      s"""
      SELECT
      "$toTableRef" AS `$toRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(toTable.data, toBinding)}
      FROM global_temp.$toTableRef"""

    val addLabelEdge: String =
      s"""
      SELECT
      "$edgeTableRef" AS `$edgeRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(edgeTable.data, edgeBinding)}
      FROM global_temp.$edgeTableRef"""

    val joinEdgeOnFrom: String =
      s"""
      SELECT * FROM ($addLabelEdge) INNER JOIN ($addLabelFrom) ON
      `$edgeRef$$${FROM_ID_COL.columnName}` = `$fromRef$$${ID_COL.columnName}`"""

    val joinEdgeOnFromAndTo: String =
      s"""
      SELECT * FROM ($joinEdgeOnFrom) INNER JOIN ($addLabelTo) ON
      `$edgeRef$$${TO_ID_COL.columnName}` = `$toRef$$${ID_COL.columnName}`"""

    SqlQuery(resQuery = joinEdgeOnFromAndTo)
  }

  private val newEdgeSchema: StructType = refactorScanSchema(edgeTable.data.schema, edgeBinding)
  private val newFromSchema: StructType = refactorScanSchema(fromTable.data.schema, fromBinding)
  private val newToSchema: StructType = refactorScanSchema(toTable.data.schema, toBinding)

  override val bindingTable: BindingTableMetadata =
    SqlBindingTableMetadata(
      sparkSchemaMap = Map(
        edgeBinding -> newEdgeSchema, fromBinding -> newFromSchema, toBinding -> newToSchema),
      sparkBtableSchema = mergeSchemas(newEdgeSchema, newFromSchema, newToSchema),
      btableOps = sqlQuery)
}
