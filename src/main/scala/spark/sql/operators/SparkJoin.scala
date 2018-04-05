package spark.sql.operators

import org.apache.spark.sql.types.StructType
import planner.target_api.{BindingTable, PhysJoin}
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

    val joinQuery: String =
      s"""
         | SELECT * FROM (${lhsBtable.btable.resQuery})
         | $joinTypeSql (${rhsBtable.btable.resQuery})
         | $joinCondition
       """.stripMargin

    val sqlJoinQuery: SqlQuery = SqlQuery(resQuery = joinQuery)

    val unifiedSchema: StructType = new StructType(mergedSchemas.toArray)

    SparkBindingTable(
      sparkSchemaMap = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      sparkBtableSchema = unifiedSchema,
      btableOps = sqlJoinQuery)
  }
}
