package spark.sql.operators

import algebra.expressions.Reference
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column.tableLabelColumn
import schema.Table

trait SparkEntityScan {
  def renameColumnsQuery(table: Table[DataFrame], ref: Reference): String = {
    table.data.columns
      .map(col => s"$col AS `${ref.refName}$$$col`")
      .mkString(", ")
  }

  def refactorSchema(schema: StructType, ref: Reference): StructType = {
    val newSchema: StructType =
      schema.foldLeft(new StructType())(
        (aggStruct, structField) => {
          aggStruct.add(structField.copy(name = s"${ref.refName}$$${structField.name}"))
        })
    newSchema.add(name = s"${ref.refName}$$${tableLabelColumn.columnName}", dataType = "String")
  }
}
