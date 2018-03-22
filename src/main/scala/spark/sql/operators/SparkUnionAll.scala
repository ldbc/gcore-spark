package spark.sql.operators

import org.apache.spark.sql.types.StructType
import planner.operators.BindingTable
import planner.target_api.PhysUnionAll
import planner.trees.TargetTreeNode

case class SparkUnionAll(lhs: TargetTreeNode, rhs: TargetTreeNode)
  extends PhysUnionAll(lhs, rhs) with SqlQueryGen {

  override val bindingTable: BindingTable = {
    val lhsBtable: SparkBindingTable = lhs.bindingTable.asInstanceOf[SparkBindingTable]
    val rhsBtable: SparkBindingTable = rhs.bindingTable.asInstanceOf[SparkBindingTable]
    val lhsSchema: StructType = lhsBtable.btableSchema
    val rhsSchema: StructType = rhsBtable.btableSchema
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

    val unionQuery: String =
      s"""
         | SELECT ${orderColumnsForUnion(lhsSchema, mergedSchemas)} FROM `$lhsTempView`
         | UNION ALL
         | SELECT ${orderColumnsForUnion(rhsSchema, mergedSchemas)} FROM `$rhsTempView`
       """.stripMargin

    val cleanupLhsTempView: String = s"DROP VIEW `$lhsTempView`"
    val cleanupRhsTempView: String = s"DROP VIEW `$rhsTempView`"

    val unionAllQueries: SqlQuery =
      SqlQuery(
        prologue =
          lhsBtable.btableOps.prologue ++ rhsBtable.btableOps.prologue
            :+ createLhsTempView :+ createRhsTempView,
        resQuery = unionQuery,
        epilogue =
          lhsBtable.btableOps.epilogue ++ rhsBtable.btableOps.epilogue
            :+ cleanupLhsTempView :+ cleanupRhsTempView)

    val unifiedSchema: StructType = new StructType(mergedSchemas.toArray)

    SparkBindingTable(
      schemas = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      btableUnifiedSchema = unifiedSchema,
      btableOps = unionAllQueries)
  }
}
