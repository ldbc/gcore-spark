package spark.sql.operators

import algebra.expressions._
import algebra.trees.AlgebraTreeNode
import algebra.types.GroupDeclaration
import common.exceptions.UnsupportedOperation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import planner.operators.Column.idColumn
import planner.target_api.{BindingTable, PhysGroupBy}
import planner.trees.TargetTreeNode
import spark.sql.operators.SqlQueryGen.expandExpression

case class SparkGroupBy(relation: TargetTreeNode,
                        groupingAttributes: Seq[AlgebraTreeNode],
                        aggregateFunctions: Seq[PropertySet],
                        having: Option[AlgebraExpression])
  extends PhysGroupBy(relation, groupingAttributes, aggregateFunctions, having) {

  private type SchemaFieldName = String

  sealed case class SchemaField(structField: StructField, selectStr: String)

  private val relationBtable: SparkBindingTable =
    relation.bindingTable.asInstanceOf[SparkBindingTable]
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

  /**
    * x { prop1 := AVG(y.prop2) } <=> x { prop1 := x.prop2 }, x.prop2 = AVG(y.prop2)
    *
    *   alias field name = x.prop2
    *   aggregate expression = AVG(y.prop2)
    *
    *   SELECT AVG(y.prop2) AS x.prop2 GROUP BY other_prop, other_prop != y.prop2
    *
    * reference x -> new StructField
    */
  private val aggregatedFields: Map[Reference, Seq[SchemaField]] = {
    aggregateFunctions
      .filter {
        case PropertySet(newVarRef, PropAssignment(propKey, expr: AggregateExpression)) =>
          expr.getOperand.isInstanceOf[PropertyRef]
      }
      .map {
        case PropertySet(newVarRef, PropAssignment(propKey, expr: AggregateExpression)) =>
          expr.getOperand match {
            // x.prop2 = AVG(y.prop2)
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
        case PropertySet(newVarRef, PropAssignment(propKey, expr: AggregateExpression)) =>
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

  override val bindingTable: BindingTable = {
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

    SparkBindingTable(
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
