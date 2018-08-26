package spark.sql

import algebra.expressions._
import algebra.operators.Column
import algebra.operators.Column._
import algebra.target_api.TargetTreeNode
import algebra.trees.AlgebraTreeNode
import common.RandomNameGenerator
import common.exceptions.UnsupportedOperation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import schema.Table
import spark.sql.operators.SqlBindingTableMetadata

/**
  * An SQL query that can additionally need a prologue and an epilogue. The parameter [[resQuery]]
  * should be the query that produces the desired result. The prologue is useful if, for example,
  * temporary views need to be registered before the [[resQuery]], while an epilogue could be
  * useful if, for example, cleanup is needed after the [[resQuery]].
  */
case class SqlQuery(prologue: Seq[String] = Seq.empty,
                    resQuery: String,
                    epilogue: Seq[String] = Seq.empty)

object SqlQuery {

  /** Create a unique alias for a table. */
  def tempViewAlias: String = RandomNameGenerator.randomString()

  /**
    * Given a table, creates the string for selecting all columns in that table and prepending the
    * binding's name to each column name. If a column's previous name was a_col and the table held
    * data for the binding v, then the new name of the column will be v$a_col.
    */
  def selectAllPrependRef(table: DataFrame, ref: Reference): String = {
    table.columns
      .map(col => s"$col AS `${ref.refName}$$$col`")
      .mkString(", ")
  }

  /**
    * Refactors the schema of a scanned entity, by prepending the binding's name to all column
    * names and by adding the extra column [[Column.TABLE_LABEL_COL]] to the schema.
    */
  def refactorScanSchema(schema: StructType, ref: Reference): StructType = {
    val newSchema: StructType =
      schema.foldLeft(new StructType())(
        (aggStruct, structField) => {
          aggStruct.add(structField.copy(name = s"${ref.refName}$$${structField.name}"))
        })
    newSchema.add(name = s"${ref.refName}$$${TABLE_LABEL_COL.columnName}", dataType = "String")
  }

  /**
    * Creates the string for selecting the columns in a table for a UNION ALL clause. The fields of
    * the give schema need to be passed to the clause in the same order for both operands, hence we
    * use a set to ensure the order. Also, for any column that is present the merged schema of the
    * two operands, but not in the given schema, we add it to the query and fill it with nulls.
    */
  def orderColumnsForUnion(schema: StructType, mergedSchemas: StructType): String = {
    val allColumns: Set[String] = mergedSchemas.fields.map(_.name).toSet
    val schemaAllColumns: Set[String] = schema.fields.map(_.name).toSet
    val missingColumns: Set[String] = allColumns.diff(schemaAllColumns)
    val queryPart: StringBuilder = StringBuilder.newBuilder

    if (missingColumns.isEmpty)
      allColumns.toArray.map(col => s"`$col`").mkString(", ")
    else
      allColumns.toArray.map(
        col => if (!missingColumns.contains(col)) s"`$col`" else s"null AS `$col`").mkString(", ")
  }

  /** Creates the string for selecting the common columns of two schemas for a JOIN clause. */
  def commonColumnsForJoin(lhsSchema: StructType, rhsSchema: StructType): String = {
    s"USING (${lhsSchema.intersect(rhsSchema).map(f => s"`${f.name}`").mkString(", ")})"
  }

  /**
    * Creates the string for selecting all columns in a schema, except for those that belong to a
    * certain binding.
    */
  def allColumnsExceptForRef(ref: Reference, schema: StructType): String = {
    schema.fields
      .map(_.name)
      .filter(!_.startsWith(ref.refName))
      .map(col => s"`$col`")
      .mkString(", ")
  }

  /** Creates a new schema, by merging together multiple schemas. */
  def mergeSchemas(schemas: StructType*): StructType = {
    new StructType(schemas.flatMap(_.fields).toSet.toArray)
  }

  /**
    * Creates a filtering predicate (a WHERE expression) from an [[AlgebraExpression]] tree. The
    * tree is traversed and the filtering expression is formed from the string representation
    * (symbol) of each node in the tree.
    *
    * The schema and alias parameters are used in case of an [[Exists]] sub-clause in the filtering
    * predicate. We need the schema to infer whether there are common attributes between the main
    * SELECT statement and the EXISTS sub-query and the alias to impose a filtering in the EXISTS
    * clause based on the equality of the common attributes.
    */
  def expressionToSelectionPred(expr: AlgebraTreeNode,
                                selectSchemaMap: Map[Reference, StructType],
                                selectAlias: String): String = {
    expr match {
      /** Expression leaves */
      case propRef: PropertyRef => s"`${propRef.ref.refName}$$${propRef.propKey.key}`"
      case StringLiteral(value) =>
        if (value.startsWith("'") && value.endsWith("'")) value else s"'$value'"
      case IntLiteral(value) => value.toString
      case Star => "*"
      case True => "True"
      case False => "False"

      /** Exists subclause. */
      case _: Exists =>
        val existsQuery: TargetTreeNode = expr.children.head.asInstanceOf[TargetTreeNode]
        val existsBtable: SqlBindingTableMetadata =
          existsQuery.bindingTable.asInstanceOf[SqlBindingTableMetadata]
        val existsSchemaMap: Map[Reference, StructType] = existsBtable.schemaMap

        val commonBindings: Set[Reference] =
          selectSchemaMap.keys.toSet.intersect(existsSchemaMap.keys.toSet)

        val tempName: String = tempViewAlias
        val id: String = ID_COL.columnName
        val selectConds: String =
          commonBindings
            .map(ref => s"$selectAlias.`${ref.refName}$$$id` = $tempName.`${ref.refName}$$$id`")
            .mkString(" AND ")

        s"""
        EXISTS
        (SELECT * FROM (${existsBtable.btable.resQuery}) $tempName WHERE $selectConds)"""

      /** Particular cases that need to be treated before generic Binary-/UnaryExpression. */
      case e: MathExpression =>
        s"${e.getSymbol}(" +
          s"${expressionToSelectionPred(e.getLhs, selectSchemaMap, selectAlias)}, " +
          s"${expressionToSelectionPred(e.getRhs, selectSchemaMap, selectAlias)})"
      case e: PredicateExpression =>
        s"${expressionToSelectionPred(e.getOperand, selectSchemaMap, selectAlias)} ${e.getSymbol}"
      // Treat GroupConcat separately, there is no direct Spark equivalent for it.
      case GroupConcat(distinct, operand) =>
        if (distinct)
          s"concat_ws(" +
            s"',', " +
            s"collect_list(" +
            s"DISTINCT ${expressionToSelectionPred(operand, selectSchemaMap, selectAlias)}))"
        else
          s"concat_ws(" +
            s"',', " +
            s"collect_list(${expressionToSelectionPred(operand, selectSchemaMap, selectAlias)})" +
            s")"
      case e: AggregateExpression =>
        if (e.isDistinct)
          s"${e.getSymbol}(" +
            s"DISTINCT ${expressionToSelectionPred(e.getOperand, selectSchemaMap, selectAlias)}" +
            s")"
        else
          s"${e.getSymbol}(" +
            s"${expressionToSelectionPred(e.getOperand, selectSchemaMap, selectAlias)}" +
            s")"

      /** Basic expressions. */
      case e: BinaryExpression =>
        s"${expressionToSelectionPred(e.getLhs, selectSchemaMap, selectAlias)} " +
          s"${e.getSymbol} " +
          s"${expressionToSelectionPred(e.getRhs, selectSchemaMap, selectAlias)}"
      case e: UnaryExpression =>
        s"${e.getSymbol} ${expressionToSelectionPred(e.getOperand, selectSchemaMap, selectAlias)}"

      /** Default case, cannot evaluate. */
      case other =>
        throw UnsupportedOperation("Cannot build filtering predicate for Spark SQL from " +
          s"expression:\n${other.treeString()}")
    }
  }

  def expandExpression(expr: AlgebraTreeNode): String =
    expressionToSelectionPred(expr, Map.empty, "")
}
