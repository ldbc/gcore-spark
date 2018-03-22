package spark.sql.operators

import org.apache.spark.sql.types.StructType
import planner.operators.BindingTable
import planner.target_api.PhysJoin
import planner.trees.TargetTreeNode

abstract class SparkJoin(lhs: TargetTreeNode, rhs: TargetTreeNode)
  extends PhysJoin(lhs, rhs) with SqlQueryGen {

  def joinTypeSql: String
  def joinCondition: String

  val lhsBtable: SparkBindingTable = lhs.bindingTable.asInstanceOf[SparkBindingTable]
  val rhsBtable: SparkBindingTable = rhs.bindingTable.asInstanceOf[SparkBindingTable]
  val lhsSchema: StructType = lhsBtable.btableSchema
  val rhsSchema: StructType = rhsBtable.btableSchema

  override val bindingTable: BindingTable = {
    val mergedSchemas: StructType = mergeSchemas(lhsSchema, rhsSchema)
    val lhsTempView: String = tempViewNameWithUID("lhs_temp")
    val rhsTempView: String = tempViewNameWithUID("rhs_temp")

    val createLhsTempView: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$lhsTempView` AS ${lhsBtable.btable.resQuery}
       """.stripMargin

    val createRhsTempView: String =
      s"""
         | CREATE OR REPLACE TEMPORARY VIEW `$rhsTempView` AS ${rhsBtable.btable.resQuery}
       """.stripMargin

    val joinQuery: String =
      s"""
         | SELECT * FROM `$lhsTempView` $joinTypeSql `$rhsTempView` $joinCondition
       """.stripMargin

    val cleanupLhsTempView: String = s"DROP VIEW `$lhsTempView`"
    val cleanupRhsTempView: String = s"DROP VIEW `$rhsTempView`"

    val joinQueries: SqlQuery =
      SqlQuery(
        prologue =
          lhsBtable.btableOps.prologue ++ rhsBtable.btableOps.prologue
            :+ createLhsTempView :+ createRhsTempView,
        resQuery = joinQuery,
        epilogue =
          lhsBtable.btableOps.epilogue ++ rhsBtable.btableOps.epilogue
            :+ cleanupLhsTempView :+ cleanupRhsTempView)

    val unifiedSchema: StructType = new StructType(mergedSchemas.toArray)

    SparkBindingTable(
      schemas = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      btableUnifiedSchema = unifiedSchema,
      btableOps = joinQueries)
  }
}
