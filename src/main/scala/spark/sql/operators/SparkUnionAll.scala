package spark.sql.operators

import org.apache.spark.sql.types.StructType
import planner.target_api.{BindingTable, PhysUnionAll}
import planner.trees.TargetTreeNode

case class SparkUnionAll(lhs: TargetTreeNode, rhs: TargetTreeNode)
  extends PhysUnionAll(lhs, rhs) with SqlQueryGen {

  override val bindingTable: BindingTable = {
    val lhsBtable: SparkBindingTable = lhs.bindingTable.asInstanceOf[SparkBindingTable]
    val rhsBtable: SparkBindingTable = rhs.bindingTable.asInstanceOf[SparkBindingTable]
    val lhsSchema: StructType = lhsBtable.btableSchema
    val rhsSchema: StructType = rhsBtable.btableSchema
    val mergedSchemas: StructType = mergeSchemas(lhsSchema, rhsSchema)

    val unionQuery: String =
      s"""
         | SELECT ${orderColumnsForUnion(lhsSchema, mergedSchemas)}
         | FROM (${lhsBtable.btable.resQuery})
         | UNION ALL
         | SELECT ${orderColumnsForUnion(rhsSchema, mergedSchemas)}
         | FROM (${rhsBtable.btable.resQuery})
       """.stripMargin

    val sqlUnionAll: SqlQuery = SqlQuery(resQuery = unionQuery)

    val unifiedSchema: StructType = new StructType(mergedSchemas.toArray)

    SparkBindingTable(
      sparkSchemaMap = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      sparkBtableSchema = unifiedSchema,
      btableOps = sqlUnionAll)
  }
}
