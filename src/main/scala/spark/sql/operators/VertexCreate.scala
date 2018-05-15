package spark.sql.operators

import algebra.operators.Column._
import algebra.operators.RemoveClause
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.SqlQuery

/**
  * Extracts the information of a vertex denoted by its [[createRule.reference]] from the construct
  * [[relation]].
  *
  * The identity of the vertex is given by its id. All the rows containing the same id will contain
  * the same values of its properties (the columns). Hence, to create the vertex, we group the
  * construct [[relation]] by the id column and use FIRST as the aggregation rule for all other
  * columns holding vertex information. By grouping, we ensure that the result contains unique
  * vertex.
  *
  * At this level in the construction process we also remove the properties and labels specified via
  * the [[createRule.removeClause]].
  *
  * Note that the result still contains the label column, as this is needed later in the creation
  * process of the graph.
  */
case class VertexCreate(relation: TargetTreeNode, createRule: target_api.VertexCreate)
  extends TargetTreeNode {

  private type SelectStr = String

  private val relationBtable: SqlBindingTableMetadata =
    relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]

  private val reference: String = createRule.reference.refName
  private val idCol: String = s"$reference$$${idColumn.columnName}"
  private val constructIdCol: String = s"$reference$$${constructIdColumn.columnName}"
  private val labelCol: String = s"$reference$$${tableLabelColumn.columnName}"

  private val idColStructField: StructField = StructField(idCol, IntegerType)

  override val bindingTable: BindingTableMetadata = {
    val existingFields: Map[StructField, SelectStr] = createExistingFieldsMap
    val newFields: Map[StructField, SelectStr] = createNewFieldsMap
    val removeFields: Set[StructField] = createRemoveFieldsSet(existingFields.keySet)

    // Aggregate everything except for the id column, which will be the key of the group by.
    val fieldsToSelect: Map[StructField, SelectStr] = (existingFields -- removeFields) ++ newFields
    val fieldsToAggregateStr: Seq[SelectStr] =
      (fieldsToSelect - idColStructField)
        .map {
          case (structField, selectStr) => s"FIRST($selectStr) AS `${structField.name}`"
        }
        .toSeq
    val columnsToSelect: String =
      (fieldsToAggregateStr :+ fieldsToSelect(idColStructField)).mkString(", ")

    val createQuery: String =
      s"""
      SELECT $columnsToSelect FROM (${relationBtable.btableOps.resQuery})
      GROUP BY `$constructIdCol`"""

    val newRefSchema: StructType = StructType(fieldsToSelect.keys.toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = Map(createRule.reference -> newRefSchema),
      sparkBtableSchema = newRefSchema,
      btableOps = SqlQuery(resQuery = createQuery))
  }

  private def createExistingFieldsMap: Map[StructField, SelectStr] = {
    Map(
      relationBtable.schemaMap(createRule.reference)
        .fields
        .map(field => field -> s"`${field.name}`"): _*)
  }

  private def createNewFieldsMap: Map[StructField, SelectStr] = {
    // We add the new id field, which is created based on the construct_id.
    val idSelect: SelectStr = s"(${createRule.tableBaseIndex} + `$constructIdCol` - 1) AS `$idCol`"

    // TODO: If the label is missing, add it as a new column and create a random name for the label.
    Map(idColStructField -> idSelect)
  }

  private def createRemoveFieldsSet(existingFields: Set[StructField]): Set[StructField] = {
    val removeClauseColumns: Set[String] = {
      if (createRule.removeClause.isDefined) {
        val removeClause: RemoveClause = createRule.removeClause.get
        val labelRemoves: Set[String] = removeClause.labelRemoves.map(_ => s"$labelCol").toSet
        val propRemoves: Set[String] =
          removeClause.propRemoves
            .map(propRemove => s"$reference$$${propRemove.propertyRef.propKey.key}")
            .toSet
        labelRemoves ++ propRemoves
      } else {
        Set.empty
      }
    }
    existingFields.filter(field =>
      // Remove the old id column and the construct id column, if they are present.
      field.name == idCol || field.name == constructIdCol ||
        // Remove all the properties and labels specified in the createRule.removeClause
        removeClauseColumns.contains(field.name)
    )
  }
}
