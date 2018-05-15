package spark.sql.operators

import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.StructType
import spark.sql.SqlQuery
import spark.sql.SqlQuery.{mergeSchemas, orderColumnsForUnion}

/**
  * Unions two relations, [[lhs]] UNION [[rhs]]. The result will contain all the columns in both
  * relations. If a column is not defined for a particular relation, the value null will be filled
  * in the column, for the row present in that relation
  */
case class UnionAll(lhs: TargetTreeNode, rhs: TargetTreeNode)
  extends target_api.UnionAll(lhs, rhs) {

  override val bindingTable: BindingTableMetadata = {
    val lhsBtable: SqlBindingTableMetadata = lhs.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val rhsBtable: SqlBindingTableMetadata = rhs.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val lhsSchema: StructType = lhsBtable.btableSchema
    val rhsSchema: StructType = rhsBtable.btableSchema
    val mergedSchemas: StructType = mergeSchemas(lhsSchema, rhsSchema)

    val unionQuery: String =
      s"""
      SELECT ${orderColumnsForUnion(lhsSchema, mergedSchemas)}
      FROM (${lhsBtable.btable.resQuery})
      UNION ALL
      SELECT ${orderColumnsForUnion(rhsSchema, mergedSchemas)}
      FROM (${rhsBtable.btable.resQuery})"""

    val sqlUnionAll: SqlQuery = SqlQuery(resQuery = unionQuery)

    SqlBindingTableMetadata(
      sparkSchemaMap = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      sparkBtableSchema = mergedSchemas,
      btableOps = sqlUnionAll)
  }
}
