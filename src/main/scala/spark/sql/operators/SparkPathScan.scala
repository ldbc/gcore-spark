package spark.sql.operators

import algebra.expressions.{Label, Reference}
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column._
import planner.operators.{BindingTable, PathScan}
import planner.target_api.PhysPathScan
import planner.trees.PlannerContext
import schema.Table

case class SparkPathScan(pathScan: PathScan, graph: Graph, plannerContext: PlannerContext)
  extends PhysPathScan(pathScan, graph, plannerContext) with SqlQueryGen {

  private val pathBinding: Reference = pathScan.pathBinding
  private val fromBinding: Reference = pathScan.fromBinding
  private val toBinding: Reference = pathScan.toBinding

  private val pathTableName: Label = pathScan.pathTableName
  private val fromTableName: Label = pathScan.fromTableName
  private val toTableName: Label = pathScan.toTableName

  private val pathTable: Table[DataFrame] =
    physGraph.tableMap(pathTableName).asInstanceOf[Table[DataFrame]]
  private val fromTable: Table[DataFrame] =
    physGraph.tableMap(fromTableName).asInstanceOf[Table[DataFrame]]
  private val toTable: Table[DataFrame] =
    physGraph.tableMap(toTableName).asInstanceOf[Table[DataFrame]]

  private val newPathSchema: StructType = {
    val refactoredSchema: StructType = refactorScanSchema(pathTable.data.schema, pathBinding)
    if (pathScan.costVarDef.isDefined)
      refactoredSchema.add(s"${pathBinding.refName}$$${pathScan.costVarDef.get.refName}", "int")
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

    val joinOnFromTempView: String = tempViewNameWithUID(s"${pathRef}_fromJoin")
    val joinOnFromAndToTempView: String = tempViewNameWithUID(s"${pathRef}_fromAndToJoin")
    val fromTempView: String = tempViewNameWithUID(s"${fromRef}_fromWithLabel")
    val toTempView: String = tempViewNameWithUID(s"${toRef}_toWithLabel")
    val pathTempView: String = tempViewNameWithUID(s"${pathRef}_pathWithLabel")

    val addLabelFrom: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$fromTempView` AS
         | SELECT
         | "$fromTableRef" AS `$fromRef$$${tableLabelColumn.columnName}`,
         | ${selectAllPrependRef(fromTable, fromBinding)}
         | FROM global_temp.$fromTableRef
       """.stripMargin

    val addLabelTo: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$toTempView` AS
         | SELECT
         | "$toTableRef" AS `$toRef$$${tableLabelColumn.columnName}`,
         | ${selectAllPrependRef(toTable, toBinding)}
         | FROM global_temp.$toTableRef
       """.stripMargin

    val addLabelPath: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$pathTempView` AS
         | SELECT
         | "$pathTableRef" AS `$pathRef$$${tableLabelColumn.columnName}`,
         | ${selectAllPrependRef(pathTable, pathBinding)}
         | FROM global_temp.$pathTableRef
       """.stripMargin

    val joinPathOnFrom: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$joinOnFromTempView` AS
         | SELECT * FROM `$pathTempView` INNER JOIN `$fromTempView` ON
         | `$pathRef$$${fromIdColumn.columnName}` = `$fromRef$$${idColumn.columnName}`
       """.stripMargin

    val reachableTestTempView: Option[String] =
      if (pathScan.isReachableTest)
        Some(
          s"""
             | CREATE OR REPLACE TEMPORARY VIEW `$joinOnFromAndToTempView` AS
             | SELECT * FROM `$joinOnFromTempView` INNER JOIN `$toTempView` ON
             | `$pathRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`
          """.stripMargin)
      else None

    val joinPathOnFromAndTo: String = {
      if (pathScan.isReachableTest) {
        s"""
           | SELECT ${allColumnsExceptForRef(pathBinding, mergedSchemas)}
           | FROM $joinOnFromAndToTempView
         """.stripMargin

      } else {
        val columns: String = {
          if (pathScan.costVarDef.isDefined)
            s"*, " +
              s"SIZE(`$pathRef$$${edgeSeqColumn.columnName}`) " +
              s"AS `$pathRef$$${pathScan.costVarDef.get.refName}`"
          else "*"
        }

        s"""
           | SELECT $columns FROM `$joinOnFromTempView` INNER JOIN `$toTempView` ON
           | `$pathRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`
          """.stripMargin
      }
    }

    val cleanupJoinOnFromTempView: String = s"DROP VIEW `$joinOnFromTempView`"
    val cleanupJoinOnFromAndToTempView: Option[String] =
      if (pathScan.isReachableTest) Some(s"DROP VIEW $joinOnFromAndToTempView")
      else None
    val cleanupFromTempView: String = s"DROP VIEW `$fromTempView`"
    val cleanupToTempView: String = s"DROP VIEW `$toTempView`"
    val cleanupPathTempView: String = s"DROP VIEW `$pathTempView`"

    SqlQuery(
      prologue =
        Seq(addLabelFrom, addLabelTo, addLabelPath, joinPathOnFrom) ++ reachableTestTempView.toList,
      resQuery = joinPathOnFromAndTo,
      epilogue =
        Seq(cleanupFromTempView, cleanupToTempView, cleanupPathTempView, cleanupJoinOnFromTempView)
          ++ cleanupJoinOnFromAndToTempView.toList
    )
  }

  override val bindingTable: BindingTable =
    SparkBindingTable(
      schemas = {
        if (pathScan.isReachableTest)
          Map(fromBinding -> newFromSchema, toBinding -> newToSchema)
        else
          Map(pathBinding -> newPathSchema, fromBinding -> newFromSchema, toBinding -> newToSchema)
      },
      btableUnifiedSchema = {
        if (pathScan.isReachableTest) mergeSchemas(newFromSchema, newToSchema)
        else mergedSchemas
      },
      btableOps = sqlQuery)
}
