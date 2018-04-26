package spark.sql.operators

import algebra.operators.Column._
import algebra.operators.StoredPathRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import schema.{GraphDb, Table}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

case class PathScan(pathRelation: StoredPathRelation, graph: Graph, graphDb: GraphDb)
  extends target_api.PathScan(pathRelation, graph, graphDb) {

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
      "$fromTableRef" AS `$fromRef$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(fromTable, fromBinding)}
      FROM global_temp.$fromTableRef"""

    val addLabelTo: String =
      s"""
      SELECT
      "$toTableRef" AS `$toRef$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(toTable, toBinding)}
      FROM global_temp.$toTableRef"""

    val addLabelPath: String =
      s"""
      SELECT
      "$pathTableRef" AS `$pathRef$$${tableLabelColumn.columnName}`,
      ${selectAllPrependRef(pathTable, pathBinding)}
      FROM global_temp.$pathTableRef"""

    val joinPathOnFrom: String =
      s"""
      SELECT * FROM ($addLabelPath) INNER JOIN ($addLabelFrom) ON
      `$pathRef$$${fromIdColumn.columnName}` = `$fromRef$$${idColumn.columnName}`"""

    val joinPathOnFromAndTo: String = {
      if (pathRelation.isReachableTest) {
        val reachableTestTempView: String =
          s"""
          SELECT * FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
          `$pathRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`"""

        s"""
        SELECT ${allColumnsExceptForRef(pathBinding, mergedSchemas)}
        FROM ($reachableTestTempView)"""

      } else {
        val columns: String = {
          if (pathRelation.costVarDef.isDefined)
            s"*, " +
              s"SIZE(`$pathRef$$${edgeSeqColumn.columnName}`) " +
              s"AS `$pathRef$$${pathRelation.costVarDef.get.refName}`"
          else "*"
        }

        s"""
        SELECT $columns FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
        `$pathRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`"""
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
