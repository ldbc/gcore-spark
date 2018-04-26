package spark.sql.operators

import algebra.expressions.Reference
import algebra.operators.Column._
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.SqlQuery

case class Project(relation: TargetTreeNode, attributes: Seq[Reference])
  extends target_api.Project(relation, attributes) {

  override val bindingTable: BindingTableMetadata = {
    val relationBtable: SqlBindingTableMetadata = relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val relationSchema: StructType = relationBtable.btableSchema
    val schemaFields: Array[StructField] = relationSchema.fields
    val schemaFieldNames: Array[String] = schemaFields.map(_.name)

    val existingColumns: Seq[Reference] =
      attributes.filter(attr => schemaFieldNames.exists(_.startsWith(attr.refName)))
    val existingColumnsSchemaFields: Map[Reference, Array[StructField]] =
      existingColumns
        .map(attr => attr -> schemaFields.filter(_.name.startsWith(attr.refName)))
        .toMap
    val existingColumnsSchemaString: Iterable[String] =
      existingColumnsSchemaFields
        .values
        .flatten
        .map(field => s"`${field.name}`")

    val newColumns: Set[Reference] =
      attributes.filter(attr => !schemaFieldNames.exists(_.startsWith(attr.refName))).toSet
    val newColumnsSchemaFields: Map[Reference, Array[StructField]] =
      newColumns
        .map(ref =>
          ref -> Array(
            StructField(
              name = s"${ref.refName}$$${idColumn.columnName}",
              dataType = IntegerType)))
        .toMap
    val newColumnsSchemaString: Set[String] =
      newColumns.map(ref => s"`${ref.refName}$$${idColumn.columnName}`")

    val columnSelectionList: String =
      (existingColumnsSchemaString ++ newColumnsSchemaString).mkString(", ")

    val sqlProject: String = {
      val table: String = {
        if (newColumns.nonEmpty) {
          // For each new variable, we add a column containing monotonically increasing id's. We
          // cannot simply add any constant here, because it will be coallesced into a single group
          // by a GROUP BY clause. The monotonic id is actually one id per partition and, for each
          // row on that partition it increases by one. Therefore, in the resulting table, we may
          // see very skewed id's in the new columns. However, they should be distinct from each
          // other and thus have all the new rows preserved after the GROUP BY.
          val selectNewVarsId: String =
            newColumns
              .map(
                ref => s"MONOTONICALLY_INCREASING_ID() AS `${ref.refName}$$${idColumn.columnName}`")
              .mkString(", ")

          s"""
          SELECT *, $selectNewVarsId FROM (${relationBtable.btable.resQuery})"""

        } else
          relationBtable.btableOps.resQuery
      }

      s"""
      SELECT $columnSelectionList FROM ($table)"""
    }

    val projectionSchema: StructType =
      StructType(
        (existingColumnsSchemaFields.values.flatten ++ newColumnsSchemaFields.values.flatten)
          .toArray)
    val projectionRefSchema: Map[Reference, StructType] =
      existingColumnsSchemaFields.mapValues(StructType(_)) ++
        newColumnsSchemaFields.mapValues(StructType(_))

    SqlBindingTableMetadata(
      sparkSchemaMap = projectionRefSchema,
      sparkBtableSchema = projectionSchema,
      btableOps = SqlQuery(resQuery = sqlProject))
  }
}
