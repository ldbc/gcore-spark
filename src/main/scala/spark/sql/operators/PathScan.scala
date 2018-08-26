package spark.sql.operators

import algebra.operators.Column._
import algebra.operators.StoredPathRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import schema.{Catalog, Table}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

/**
  * Creates the table that will hold information about a stored path and its endpoints.
  *
  * Each path and vertex type (denoted by the entity label) is stored into a separate table in the
  * database. For example, for the path (a)-/@p/->(b), we will need three tables, a's, p's and b's,
  * to create the result of the [[PathScan]] operation. To do this, we first create temporary views
  * over a's, b's and p's tables. We then join p's table with a's on p.fromid == a.id and the result
  * with b's table on p.toid == b.id.
  *
  * If the path is a reachability test ([[pathRelation.isReachableTest]] = true), we will only keep
  * a's and b's properties in the resulting table.
  *
  * If the cost of the path is required ([[pathRelation.costVarDef]] is defined), then the length
  * of the path's [[EDGE_SEQ_COL]] entries is added to the path's row aliased as
  * [[pathRelation.costVarDef]].
  */
case class PathScan(pathRelation: StoredPathRelation, graph: Graph, catalog: Catalog)
  extends target_api.PathScan(pathRelation, graph, catalog) {

  private val pathTable: Table[DataFrame] =
    physGraph.tableMap(pathTableName).asInstanceOf[Table[DataFrame]]
  private val fromTable: Table[DataFrame] =
    physGraph.tableMap(fromTableName).asInstanceOf[Table[DataFrame]]
  private val toTable: Table[DataFrame] =
    physGraph.tableMap(toTableName).asInstanceOf[Table[DataFrame]]

  private val newPathSchema: StructType = {
    val refactoredSchema: StructType = refactorScanSchema(pathTable.data.schema, pathBinding)
    if (pathRelation.costVarDef.isDefined)
      refactoredSchema.add(s"${pathBinding.refName}$$${pathRelation.costVarDef.get.refName}", "int")
    else
      refactoredSchema
  }
  private val newFromSchema: StructType = refactorScanSchema(fromTable.data.schema, fromBinding)
  private val newToSchema: StructType = refactorScanSchema(toTable.data.schema, toBinding)
  private val mergedSchemas: StructType = mergeSchemas(newPathSchema, newFromSchema, newToSchema)

  private val sqlQuery: SqlQuery = {
    pathTable.data.createOrReplaceGlobalTempView(pathTableName.value)
    fromTable.data.createOrReplaceGlobalTempView(fromTableName.value)
    toTable.data.createOrReplaceGlobalTempView(toTableName.value)

    val pathRef: String = pathBinding.refName
    val fromRef: String = fromBinding.refName
    val toRef: String = toBinding.refName

    val pathTableRef: String = pathTableName.value
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

    val addLabelPath: String =
      s"""
      SELECT
      "$pathTableRef" AS `$pathRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(pathTable.data, pathBinding)}
      FROM global_temp.$pathTableRef"""

    val joinPathOnFrom: String =
      s"""
      SELECT * FROM ($addLabelPath) INNER JOIN ($addLabelFrom) ON
      `$pathRef$$${FROM_ID_COL.columnName}` = `$fromRef$$${ID_COL.columnName}`"""

    val joinPathOnFromAndTo: String = {
      if (pathRelation.isReachableTest) {
        val reachableTestTempView: String =
          s"""
          SELECT * FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
          `$pathRef$$${TO_ID_COL.columnName}` = `$toRef$$${ID_COL.columnName}`"""

        s"""
        SELECT ${allColumnsExceptForRef(pathBinding, mergedSchemas)}
        FROM ($reachableTestTempView)"""

      } else {
        val columns: String = {
          if (pathRelation.costVarDef.isDefined)
            s"*, " +
              s"SIZE(`$pathRef$$${EDGE_SEQ_COL.columnName}`) " +
              s"AS `$pathRef$$${pathRelation.costVarDef.get.refName}`"
          else "*"
        }

        s"""
        SELECT $columns FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
        `$pathRef$$${TO_ID_COL.columnName}` = `$toRef$$${ID_COL.columnName}`"""
      }
    }

    SqlQuery(resQuery = joinPathOnFromAndTo)
  }

  override val bindingTable: BindingTableMetadata =
    SqlBindingTableMetadata(
      sparkSchemaMap = {
        if (pathRelation.isReachableTest)
          Map(fromBinding -> newFromSchema, toBinding -> newToSchema)
        else
          Map(pathBinding -> newPathSchema, fromBinding -> newFromSchema, toBinding -> newToSchema)
      },
      sparkBtableSchema = {
        if (pathRelation.isReachableTest) mergeSchemas(newFromSchema, newToSchema)
        else mergedSchemas
      },
      btableOps = sqlQuery)
}
