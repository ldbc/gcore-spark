package spark.sql.operators

import algebra.expressions._
import algebra.operators.Column._
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import algebra.trees.AlgebraTreeNode
import algebra.types.GroupDeclaration
import common.exceptions.UnsupportedOperation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

/**
  * Groups the [[relation]] by the [[groupingAttributes]], which can be [[Reference]]s or a
  * [[GroupDeclaration]]. We only accept [[PropertyRef]]erences as the attributes specified within a
  * [[GroupDeclaration]]. Particular column aggregations can be specified as [[aggregateFunctions]].
  * All other columns that are neither part of the aggregation key, nor a particular aggregation
  * in the [[aggregateFunctions]], will be aggregated with the FIRST function.
  *
  * For now, the [[having]] parameter is ignored.
  */
case class GroupBy(relation: TargetTreeNode,
                   groupingAttributes: Seq[AlgebraTreeNode],
                   aggregateFunctions: Seq[PropertySet],
                   having: Option[AlgebraExpression])
  extends target_api.GroupBy(relation, groupingAttributes, aggregateFunctions, having) {

  private type SchemaFieldName = String

  sealed case class SchemaField(structField: StructField, selectStr: String)

  private val relationBtable: SqlBindingTableMetadata =
    relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
  private val relationSchema: StructType = relationBtable.btableSchema
  private val relationSchemaMap: Map[Reference, StructType] = relationBtable.schemaMap

  /**
    * x GROUP c.prop => SELECT ... GROUP BY c.prop
    */
  private val groupByFields: Map[SchemaFieldName, SchemaField] = {
    groupingAttributes
      .flatMap {
        case ref: Reference => Seq(PropertyRef(ref, PropertyKey(idColumn.columnName)))
        case groupDecl: GroupDeclaration =>
          groupDecl.children.map {
            case propRef: PropertyRef => propRef
            case other: AlgebraTreeNode =>
              throw UnsupportedOperation(s"Cannot use ${other.name} in a GroupDeclaration.")
          }
        case other: AlgebraTreeNode =>
          throw UnsupportedOperation(s"Cannot use ${other.name} in a group by clause.")
      }
      .map(propRef => {
        val fieldName: SchemaFieldName = s"${propRef.ref.refName}$$${propRef.propKey.key}"
        val schemaField: SchemaField =
          SchemaField(
            structField =
              relationSchemaMap(propRef.ref).find(structField => structField.name == fieldName).get,
            selectStr = s"`$fieldName`")
        fieldName -> schemaField
      })
      .toMap
  }

  private val ungroupedFields: Set[SchemaField] = {
    relationSchema.fields
      .filter(field => !groupByFields.keySet.contains(field.name))
      .map(field =>
        SchemaField(
          structField = field,
          selectStr = s"FIRST(`${field.name}`) AS `${field.name}`")
      )
      .toSet
  }

  private val aggregatedFields: Map[Reference, Seq[SchemaField]] = {
    aggregateFunctions
      .filter {
        case PropertySet(newVarRef, PropAssignment(propKey, expr: AggregateExpression)) =>
          expr.getOperand.isInstanceOf[PropertyRef]
      }
      .map {
        case PropertySet(newVarRef, PropAssignment(propKey, expr: AggregateExpression)) =>
          expr.getOperand match {
            // eg: x.prop2 = AVG(y.prop2)
            case PropertyRef(aggVarRef, PropertyKey(key)) =>
              val aggFieldName: SchemaFieldName = s"${aggVarRef.refName}$$$key" // y$prop2
              val newPropFieldName: String = s"${newVarRef.refName}$$${propKey.key}"  // x$prop2

              val aggExprStr: String = expandExpression(expr) // AVG(y$prop2)
              val selectString: String = s"$aggExprStr AS `$newPropFieldName`"

              val aggStructField: StructField =
                relationSchemaMap(aggVarRef).find(_.name == aggFieldName).get
              val newSchemaField: SchemaField =
                SchemaField(
                  StructField(newPropFieldName, aggStructField.dataType),
                  selectString)

              newVarRef -> newSchemaField
          }
      }
      .groupBy(refSchemaFieldTuple => refSchemaFieldTuple._1)
      .mapValues(seqTuples => seqTuples.map(refSchemaFieldTuple => refSchemaFieldTuple._2))
  }

  private val starAggregations: Map[Reference, Seq[SchemaField]] = {
    aggregateFunctions
      .filter {
        case PropertySet(_, PropAssignment(_, expr: AggregateExpression)) =>
          !expr.getOperand.isInstanceOf[PropertyRef]
      }
      .map {
        case PropertySet(newVarRef, PropAssignment(propKey, expr: AggregateExpression)) =>
          val newPropFieldName: String = s"${newVarRef.refName}$$${propKey.key}"  // x$prop2

          val aggExprStr: String = expandExpression(expr) // COUNT(*)
          val selectString: String = s"$aggExprStr AS `$newPropFieldName`"

          val newSchemaField: SchemaField =
            SchemaField(
              StructField(newPropFieldName, IntegerType), // TODO: Most likely integer, but always?
              selectString)

          newVarRef -> newSchemaField
      }
      .groupBy(refSchemaFieldTuple => refSchemaFieldTuple._1)
      .mapValues(seqTuples => seqTuples.map(refSchemaFieldTuple => refSchemaFieldTuple._2))
  }

  override val bindingTable: BindingTableMetadata = {
    val groupByQuery: String = {
      if (groupingAttributes.isEmpty) {
        s"""
        SELECT * FROM (${relationBtable.btable.resQuery})"""

      } else {
        val selectColumns: String =
          (groupByFields.values.map(_.selectStr) ++
            ungroupedFields.map(_.selectStr) ++
            aggregatedFields.flatMap(refSchemaFields => refSchemaFields._2).map(_.selectStr) ++
            starAggregations.flatMap(refSchemaFields => refSchemaFields._2).map(_.selectStr))
            .mkString(", ")

        val groupByColumns: String =
          groupByFields
            .values
            .map(schemaField => schemaField.selectStr)
            .mkString(", ")

        s"""
        SELECT $selectColumns FROM (${relationBtable.btable.resQuery}) GROUP BY $groupByColumns"""
      }
    }

    val newSchemaMap: Map[Reference, StructType] = createNewSchemaMap
    val newBtableSchema: StructType = StructType(newSchemaMap.values.flatMap(_.fields).toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = newSchemaMap,
      sparkBtableSchema = newBtableSchema,
      btableOps = SqlQuery(resQuery = groupByQuery))
  }

  private def createNewSchemaMap: Map[Reference, StructType] = {
    val existingFieldsSchemaMap: Map[Reference, StructType] =
      relationSchemaMap.map {
        case (ref, refSchema) =>
          val newAggregatedFields: Seq[StructField] =
            aggregatedFields.getOrElse(ref, Seq.empty).map(schemaField => schemaField.structField)
          val newStarAggregations: Seq[StructField] =
            starAggregations.getOrElse(ref, Seq.empty).map(schemaField => schemaField.structField)

          if (newAggregatedFields.isEmpty && newStarAggregations.isEmpty)
            ref -> refSchema
          else
            ref -> StructType(refSchema.fields ++ newAggregatedFields ++ newStarAggregations)
      }

    val newFieldsSchemaMap: Map[Reference, StructType] =
      (aggregatedFields.keySet ++ starAggregations.keySet)
        .filter(ref => !relationSchemaMap.contains(ref))
        .map(ref =>
          ref ->
            StructType(
              aggregatedFields.getOrElse(ref, Seq.empty).map(_.structField) ++
                starAggregations.getOrElse(ref, Seq.empty).map(_.structField)))
        .toMap

    existingFieldsSchemaMap ++ newFieldsSchemaMap
  }
}
