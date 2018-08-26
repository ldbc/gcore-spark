package spark.sql.operators

import algebra.expressions.DisjunctLabels
import algebra.operators.Column.{FROM_ID_COL, ID_COL, TABLE_LABEL_COL, TO_ID_COL}
import algebra.operators.VirtualPathRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.{Graph, KleeneStar}
import common.exceptions.UnsupportedOperation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.{Catalog, Table}
import spark.graphx.Utils.createPathData
import spark.sql.SqlQuery
import spark.sql.SqlQuery.{mergeSchemas, refactorScanSchema, selectAllPrependRef}

/**
  * Creates a table that will store information about shortest paths discovered in the graph between
  * two given vertex types.
  */
case class PathSearch(pathRelation: VirtualPathRelation,
                      graph: Graph,
                      catalog: Catalog,
                      sparkSession: SparkSession)
  extends target_api.PathSearch(pathRelation, graph, catalog) {

  override val bindingTable: BindingTableMetadata = {
    val fromData: DataFrame =
      physGraph.tableMap(fromTableName).asInstanceOf[Table[DataFrame]].data
    val toData: DataFrame =
      physGraph.tableMap(toTableName).asInstanceOf[Table[DataFrame]].data
    val edgeData: DataFrame = pathRelation.pathExpression match {
      case Some(KleeneStar(DisjunctLabels(Seq(edgeTableName)), _, _)) =>
        physGraph.tableMap(edgeTableName).asInstanceOf[Table[DataFrame]].data
      case _ =>
        throw UnsupportedOperation("Unsupported path configuration.")
    }
    val pathData: DataFrame = createPathData(edgeData, fromData, toData, sparkSession)

    fromData.createOrReplaceGlobalTempView(fromTableName.value)
    toData.createOrReplaceGlobalTempView(toTableName.value)
    pathData.createOrReplaceGlobalTempView(s"vpath_${pathRelation.ref.refName}")

    val pathRef: String = pathBinding.refName
    val fromRef: String = fromBinding.refName
    val toRef: String = toBinding.refName
    val fromTableRef: String = fromTableName.value
    val toTableRef: String = toTableName.value

    val selectPath: String =
      s"""
      SELECT ${selectAllPrependRef(pathData, pathRelation.ref)}
      FROM global_temp.vpath_${pathRelation.ref.refName}"""

    val addLabelFrom: String =
      s"""
      SELECT "$fromTableRef" AS `$fromRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(fromData, fromBinding)} FROM global_temp.$fromTableRef"""

    val addLabelTo: String =
      s"""
      SELECT "$toTableRef" AS `$toRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(toData, toBinding)} FROM global_temp.$toTableRef"""

    val joinPathOnFrom: String =
      s"""
      SELECT * FROM ($selectPath) INNER JOIN ($addLabelFrom) ON
      `$pathRef$$${FROM_ID_COL.columnName}` = `$fromRef$$${ID_COL.columnName}`"""

    val joinPathOnFromAndTo: String =
      s"""
      SELECT * FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
      `$pathRef$$${TO_ID_COL.columnName}` = `$toRef$$${ID_COL.columnName}`"""

    val newPathSchema: StructType = refactorScanSchema(pathData.schema, pathRelation.ref)
    val newFromSchema: StructType = refactorScanSchema(fromData.schema, pathRelation.fromRel.ref)
    val newToSchema: StructType = refactorScanSchema(toData.schema, pathRelation.toRel.ref)

    SqlBindingTableMetadata(
      sparkSchemaMap = Map(
        pathRelation.ref -> newPathSchema,
        pathRelation.fromRel.ref -> newFromSchema,
        pathRelation.toRel.ref -> newToSchema),
      sparkBtableSchema = mergeSchemas(newPathSchema, newFromSchema, newToSchema),
      btableOps = SqlQuery(resQuery = joinPathOnFromAndTo))
  }
}
