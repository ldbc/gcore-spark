package spark

import algebra.expressions.{Label, PropertyKey}
import algebra.types._
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import schema._

/**
  * A [[PathPropertyGraph]] that uses Spark's [[DataFrame]]s to store graph data. Each table in the
  * data model of the graph is backed by a [[DataFrame]]. The [[GraphSchema]] is inferred from the
  * structure of each [[DataFrame]], so an extending class should typically define (in its
  * particular way) the [[schema.GraphData.vertexData]], [[schema.GraphData.edgeData]] and
  * [[schema.GraphData.pathData]].
  */
abstract class SparkGraph extends PathPropertyGraph[DataFrame] {

  override def vertexSchema: EntitySchema = buildSchema(vertexData)

  override def edgeSchema: EntitySchema = buildSchema(edgeData)

  override def pathSchema: EntitySchema = buildSchema(pathData)

  /**
    * Infers the schema of an entity type (vertex, edge, path) from the sequence of data [[Table]]s
    * comprising this graph.
    */
  private def buildSchema(data: Seq[Table[DataFrame]]): EntitySchema =
    data.foldLeft(EntitySchema.empty) {
      case (aggSchema, table) => {
        val schemaFields = table.data.schema.fields
        val schemaMap = schemaFields.foldLeft(SchemaMap.empty[PropertyKey, DataType[_]]) {
          case (aggMap, field) =>
            aggMap union SchemaMap(Map(PropertyKey(field.name) -> convertType(field.dataType)))
        }

        aggSchema union EntitySchema(SchemaMap(Map(Label(table.name) -> schemaMap)))
      }
    }

  /** Maps a [[sql.types.DataType]] to an algebraic [[DataType]]. */
  // TODO: Check that the array type can only be the sequence of edges that define a path.
  private def convertType(sparkType: sql.types.DataType): DataType[_] = {
    sparkType.typeName match {
      case "string" => TypeString()
      case "integer" => TypeInteger()
      case "long" => TypeInteger()
      case "double" => TypeDecimal()
      case "float" => TypeDecimal()
      case "boolean" => TypeBoolean()
      case "array" => TypeArray()
      case other => throw SparkException(s"Unsupported type $other")
    }
  }
}
