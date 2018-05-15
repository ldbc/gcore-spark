package spark.sql.operators

import algebra.expressions._
import algebra.operators.Column._
import algebra.operators.{RemoveClause, SetClause}
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import algebra.trees.BasicToGroupConstruct
import org.apache.spark.sql.types._
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

import scala.collection.mutable

/**
  * Given a construct [[relation]], will select the necessary columns, possibly adding new ones,
  * such that the result contains all the columns of the new entity denoted by its [[reference]] and
  * possibly other columns as well, that will be needed in later stages of the creation process.
  *
  * New properties or labels for this entity can be added via the a [[SetClause]].
  *
  * Each new instance of this entity, denoted by a row in the construct [[relation]], will receives
  * a unique, strictly increasing id. To this end, we use SQL's ROW_NUMBER() function applied over
  * the [[relation]] ordered by the entity's previous id. The new column with consecutive ids is
  * aliased with the [[constructIdColumn]] name.
  *
  * If this was an unmatched ([[isMatchedRef]] = false), but grouped ([[groupedAttributes]]
  * non-empty) entity, we remove all the attributes of the [[relation]] that are not properties of
  * the new entity, <b>except</b> for the [[groupedAttributes]]. For more details, see
  * [[BasicToGroupConstruct]].
  *
  * TODO: The [[removeClause]] parameter is redundant and should be removed.
  * TODO: Treat the case of multiple labels or missing labels.
  */
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

    val idColumnName: String = s"${reference.refName}$$${idColumn.columnName}"
    val constructIdColumnName: String = s"${reference.refName}$$${constructIdColumn.columnName}"
    val constructIdColumnStructField: StructField = StructField(constructIdColumnName, IntegerType)

    val createQuery: String =
      s"""
      SELECT ROW_NUMBER() OVER (ORDER BY `$idColumnName`) AS `$constructIdColumnName`,
      $columnsToSelect FROM (${relationBtable.btableOps.resQuery})"""

    val newSchemaMap: Map[Reference, StructType] =
      (fieldsToSelect + constructIdColumnStructField)
        .map(structField => {
          val reference: Reference = Reference(structField.name.split("\\$")(0))
          reference -> structField
        })
        .groupBy(refStructFieldTuple => refStructFieldTuple._1) // group by reference
        .mapValues(refStructFieldTuples => refStructFieldTuples.map(_._2))
        .mapValues(structFields => StructType(structFields.toArray))
    val newBtableSchema: StructType = StructType(newSchemaMap.values.flatMap(_.fields).toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = newSchemaMap,
      sparkBtableSchema = newBtableSchema,
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
            newFieldsToSelectStr += (StructField(columnName, StringType) -> selectStr)
          case PropAssignment(propKey, propExpr) =>
            val columnName = s"${reference.refName}$$${propKey.key}"
            val selectStr = s"(${expandExpression(propExpr)}) AS `$columnName`"
            // TODO: Infer the correct data type.
            newFieldsToSelectStr += (StructField(columnName, StringType) -> selectStr)
          case _ =>
        })

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
