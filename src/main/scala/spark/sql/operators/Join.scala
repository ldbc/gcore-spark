package spark.sql.operators

import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.StructType
import spark.sql.SqlQuery
import SqlQuery.mergeSchemas

/** A generic join of two relations: [[lhs]] JOIN [[rhs]]. */
abstract class Join(lhs: TargetTreeNode, rhs: TargetTreeNode) extends target_api.Join(lhs, rhs) {

  /** The SQL keyword for this [[Join]]. Example: INNER JOIN, CROSS JOIN, etc. */
  def joinTypeSql: String

  /**
    * The condition on which to join. Must be expressed as a valid SQL join condition. Example:
    * ON lhs.id = rhs.id, USING (id), etc.
    */
  def joinCondition: String

  val lhsBtable: SqlBindingTableMetadata = lhs.bindingTable.asInstanceOf[SqlBindingTableMetadata]
  val rhsBtable: SqlBindingTableMetadata = rhs.bindingTable.asInstanceOf[SqlBindingTableMetadata]
  val lhsSchema: StructType = lhsBtable.btableSchema
  val rhsSchema: StructType = rhsBtable.btableSchema

  override val bindingTable: BindingTableMetadata = {
    val mergedSchemas: StructType = mergeSchemas(lhsSchema, rhsSchema)

    val joinQuery: String =
      s"""
      SELECT * FROM (${lhsBtable.btable.resQuery})
      $joinTypeSql (${rhsBtable.btable.resQuery})
      $joinCondition"""

    val sqlJoinQuery: SqlQuery = SqlQuery(resQuery = joinQuery)

    val unifiedSchema: StructType = new StructType(mergedSchemas.toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      sparkBtableSchema = unifiedSchema,
      btableOps = sqlJoinQuery)
  }
}
