package spark.sql.operators

import algebra.expressions._
import algebra.operators.Column._
import algebra.operators.{RemoveClause, SetClause}
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types._
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

import scala.collection.mutable

// TODO: We should verify here again that the vertex does not end up with two labels. What happens
// if we add two labels and remove one?
case class EntityConstruct(reference: Reference,
                           isMatchedRef: Boolean,
                           relation: TargetTreeNode,
                           groupedAttributes: Seq[PropertyRef],
                           expr: ObjectConstructPattern,
                           setClause: Option[SetClause],
                           removeClause: Option[RemoveClause])
  extends target_api.EntityConstruct(
    reference, isMatchedRef, relation, groupedAttributes, expr, setClause, removeClause) {

  private type StructFieldName = String
  private type SelectStr = String

  private val relationBtable: SqlBindingTableMetadata =
    relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]

  override val bindingTable: BindingTableMetadata = {
    val existingFields: Map[StructField, SelectStr] = createExistingFieldsMap
    val newFields: Map[StructField, SelectStr] = createNewFieldsMap
    val existingAndNewFields: Map[StructField, SelectStr] = existingFields ++ newFields
    val removeFields: Set[StructField] = createRemoveFieldsSet(existingAndNewFields.keySet)

    val fieldsToSelect: Set[StructField] = existingAndNewFields.keySet -- removeFields
    val columnsToSelect: String = fieldsToSelect.map(existingAndNewFields).mkString(", ")

    val newRefSchema: StructType = StructType(fieldsToSelect.toArray)

    // TODO: If the label is missing, add it as a new column and create a random name for the label.
    // TODO: Same as above for id.

    val createQuery: String =
      s"""
      SELECT $columnsToSelect FROM (${relationBtable.btableOps.resQuery})"""

    SqlBindingTableMetadata(
      sparkSchemaMap = Map(reference -> newRefSchema),
      sparkBtableSchema = newRefSchema,
      btableOps = SqlQuery(resQuery = createQuery))
  }

  private def createExistingFieldsMap: Map[StructField, SelectStr] = {
    relationBtable.btableSchema.fields
      .map(field => field -> s"`${field.name}`")
      .toMap
  }

  private def createNewFieldsMap: Map[StructField, SelectStr] = {
    val newFieldsToSelectStr: mutable.ArrayBuffer[(StructField, SelectStr)] =
      new mutable.ArrayBuffer[(StructField, SelectStr)]()

    // For each property or label assignment, we add it as a new field to this vertex's schema.
    (expr +: setClause.toList)
      .foreach(
        _.forEachDown {
          case label: Label =>
            val columnName = s"${reference.refName}$$${tableLabelColumn.columnName}"
            val selectStr = s""""${label.value}" AS `$columnName`"""
            newFieldsToSelectStr += Tuple2(StructField(columnName, StringType), selectStr)
          case PropAssignment(propKey, propExpr) =>
            val columnName = s"${reference.refName}$$${propKey.key}"
            val selectStr = s"(${expandExpression(propExpr)}) AS `$columnName`"
            // TODO: Infer the correct data type.
            newFieldsToSelectStr += Tuple2(StructField(columnName, StringType), selectStr)
          case _ =>
        })

    val idColumnName: String = s"${reference.refName}$$${idColumn.columnName}"
    val hasId: Boolean = relationBtable.schemaMap(reference).exists(_.name == idColumnName)

    if (!hasId)
      newFieldsToSelectStr +=
        Tuple2(
          StructField(idColumnName, IntegerType),
          s"MONOTONICALLY_INCREASING_ID() AS `$idColumnName`")

    newFieldsToSelectStr.toMap
  }

  private def createRemoveFieldsSet(existingAndNewFields: Set[StructField]): Set[StructField] = {
    val fieldNameToType: Map[StructFieldName, DataType] =
      existingAndNewFields
        .map(field => field.name -> field.dataType)
        .toMap

    val removeFieldsArray: mutable.ArrayBuffer[StructField] = new mutable.ArrayBuffer[StructField]()
    removeClause.toList
      .foreach(
        _.forEachDown {
          case _: Label =>
            val columnName = s"${reference.refName}$$${tableLabelColumn.columnName}"
            removeFieldsArray += StructField(columnName, StringType)
          case PropertyRef(_, propKey) =>
            val columnName = s"${reference.refName}$$${propKey.key}"
            removeFieldsArray += StructField(columnName, fieldNameToType(columnName))
          case _ =>
        })

    if (!isMatchedRef && groupedAttributes.nonEmpty) {
      // If not a matched reference, but with groupings, we remove all attributes from its table,
      // except for the ones that have been used during the grouping, such that, in case of edge
      // construction, the joins with the original binding table make sense. After grouping by
      // various properties, the other aggregates lose their original meaning, so rows will be lost
      // after the join. To avoid this, we eliminate all skewed attributes.
      val groupedFields: Set[StructField] =
        groupedAttributes.map {
          propRef: PropertyRef =>
            val columnName = s"${propRef.ref.refName}$$${propRef.propKey.key}"
            StructField(columnName, fieldNameToType(columnName))
        }
        .toSet

      val nonRefFields: Set[StructField] =
        existingAndNewFields.filter(field => !field.name.startsWith(reference.refName))

      nonRefFields.foreach {
        case field: StructField if !groupedFields.contains(field) =>
          removeFieldsArray += field
        case _ =>
      }
    }

    removeFieldsArray.toSet
  }
}
