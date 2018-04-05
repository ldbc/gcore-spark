package spark.sql.operators

import algebra.expressions.{Label, Reference}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column._
import planner.operators.PathScan
import planner.target_api.{BindingTable, PhysPathScan}
import schema.Table

case class SparkPathScan(pathScan: PathScan) extends PhysPathScan(pathScan) with SqlQueryGen {

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

    val addLabelFrom: String =
      s"""
         | SELECT
         | "$fromTableRef" AS `$fromRef$$${tableLabelColumn.columnName}`,
         | ${selectAllPrependRef(fromTable, fromBinding)}
         | FROM global_temp.$fromTableRef
       """.stripMargin

    val addLabelTo: String =
      s"""
         | SELECT
         | "$toTableRef" AS `$toRef$$${tableLabelColumn.columnName}`,
         | ${selectAllPrependRef(toTable, toBinding)}
         | FROM global_temp.$toTableRef
       """.stripMargin

    val addLabelPath: String =
      s"""
         | SELECT
         | "$pathTableRef" AS `$pathRef$$${tableLabelColumn.columnName}`,
         | ${selectAllPrependRef(pathTable, pathBinding)}
         | FROM global_temp.$pathTableRef
       """.stripMargin

    val joinPathOnFrom: String =
      s"""
         | SELECT * FROM ($addLabelPath) INNER JOIN ($addLabelFrom) ON
         | `$pathRef$$${fromIdColumn.columnName}` = `$fromRef$$${idColumn.columnName}`
       """.stripMargin

    val joinPathOnFromAndTo: String = {
      if (pathScan.isReachableTest) {
        val reachableTestTempView: String =
          s"""
             | SELECT * FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
             | `$pathRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`
          """.stripMargin

        s"""
           | SELECT ${allColumnsExceptForRef(pathBinding, mergedSchemas)}
           | FROM ($reachableTestTempView)
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
           | SELECT $columns FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
           | `$pathRef$$${toIdColumn.columnName}` = `$toRef$$${idColumn.columnName}`
          """.stripMargin
      }
    }

    SqlQuery(resQuery = joinPathOnFromAndTo)
  }

  override val bindingTable: BindingTable =
    SparkBindingTable(
      sparkSchemaMap = {
        if (pathScan.isReachableTest)
          Map(fromBinding -> newFromSchema, toBinding -> newToSchema)
        else
          Map(pathBinding -> newPathSchema, fromBinding -> newFromSchema, toBinding -> newToSchema)
      },
      sparkBtableSchema = {
        if (pathScan.isReachableTest) mergeSchemas(newFromSchema, newToSchema)
        else mergedSchemas
      },
      btableOps = sqlQuery)
}
