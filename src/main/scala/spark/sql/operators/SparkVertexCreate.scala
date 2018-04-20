package spark.sql.operators

import algebra.expressions._
import algebra.operators.{RemoveClause, SetClause}
import org.apache.spark.sql.types._
import planner.operators.Column.{idColumn, tableLabelColumn}
import planner.target_api.{BindingTable, PhysVertexCreate}
import planner.trees.TargetTreeNode
import spark.sql.operators.SqlQueryGen.expandExpression

import scala.collection.mutable

// TODO: We should verify here again that the vertex does not end up with two labels. What happens
// if we add two labels and remove one?
case class SparkVertexCreate(reference: Reference,
                             relation: TargetTreeNode,
                             expr: ObjectConstructPattern,
                             setClause: Option[SetClause],
                             removeClause: Option[RemoveClause])
  extends PhysVertexCreate(reference, relation, expr, setClause, removeClause) {

  private type StructFieldName = String
  private type SelectStr = String

  private val relationBtable: SparkBindingTable =
    relation.bindingTable.asInstanceOf[SparkBindingTable]

  override val bindingTable: BindingTable = {
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

    SparkBindingTable(
      sparkSchemaMap = Map(reference -> newRefSchema),
      sparkBtableSchema = newRefSchema,
      btableOps = SqlQuery(resQuery = createQuery))
  }

  private def createExistingFieldsMap: Map[StructField, SelectStr] = {
    relationBtable.schemaMap(reference).fields
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

    removeFieldsArray.toSet
  }
}
