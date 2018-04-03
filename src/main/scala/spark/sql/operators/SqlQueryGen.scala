package spark.sql.operators

import algebra.expressions._
import algebra.trees.AlgebraTreeNode
import algebra.types.{GcoreInteger, GcoreString}
import org.apache.commons.text.RandomStringGenerator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import planner.operators.Column
import planner.operators.Column.tableLabelColumn
import schema.Table
import spark.exceptions.UnsupportedOperation

object SqlQueryGen extends SqlQueryGen

trait SqlQueryGen {

  val randomStringLength: Int = 5
  val randomStringGenerator: RandomStringGenerator =
    new RandomStringGenerator.Builder().selectFrom(('0' to '9') ++ ('a' to 'z'): _*).build()

  /**
    * Appends a random string of length [[randomStringLength]] to the name of a temporary view. This
    * ensures that the view's name will be unique throughout the query session.
    */
  def tempViewNameWithUID(namePart: String): String = {
    s"${namePart}_${randomStringGenerator.generate(randomStringLength)}"
  }

  /**
    * Given a [[Table]], creates the string for selecting all columns in that [[Table]] and
    * prepending the binding's name to each column name. If a column's previous name was a_col and
    * the table held data for the binding v, then the new name of the column will be v$a_col.
    */
  def selectAllPrependRef(table: Table[DataFrame], ref: Reference): String = {
    table.data.columns
      .map(col => s"$col AS `${ref.refName}$$$col`")
      .mkString(", ")
  }

  /**
    * Refactors the schema of a scanned entity, by prepending the binding's name to all column
    * names and by adding the extra column [[Column.tableLabelColumn]] to the schema.
    */
  def refactorScanSchema(schema: StructType, ref: Reference): StructType = {
    val newSchema: StructType =
      schema.foldLeft(new StructType())(
        (aggStruct, structField) => {
          aggStruct.add(structField.copy(name = s"${ref.refName}$$${structField.name}"))
        })
    newSchema.add(name = s"${ref.refName}$$${tableLabelColumn.columnName}", dataType = "String")
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
  def allColumnsExceptForRef(ref: Reference, mergeSchemas: StructType): String = {
    mergeSchemas.fields
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
    */
  def expressionToSelectionPred(expr: AlgebraTreeNode): String = {
    expr match {
      /** Expression leaves */
      case propRef: PropertyRef => s"`${propRef.ref.refName}$$${propRef.propKey.key}`"
      case Literal(value, GcoreString()) => s"'$value'"
      case Literal(value, GcoreInteger()) => value.toString
      case True() => "True"
      case False() => "False"

      case e: MathExpression =>
        s"${e.getSymbol}(" +
          s"${expressionToSelectionPred(e.getLhs)}, " +
          s"${expressionToSelectionPred(e.getRhs)})"
      case e: PredicateExpression =>
        s"${expressionToSelectionPred(e.getOperand)} ${e.getSymbol}"
      case e: BinaryExpression =>
        s"${expressionToSelectionPred(e.getLhs)} " +
          s"${e.getSymbol} " +
          s"${expressionToSelectionPred(e.getRhs)}"
      case e: UnaryExpression =>
        s"${e.getSymbol} ${expressionToSelectionPred(e.getOperand)}"

      /** Default case, cannot evaluate. */
      case other =>
        throw UnsupportedOperation("Cannot build filtering predicate for Spark SQL from " +
          s"expression:\n${other.treeString()}")
    }
  }
}
