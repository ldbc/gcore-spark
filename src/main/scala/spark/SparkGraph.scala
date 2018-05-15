package spark

import algebra.expressions.PropertyKey
import algebra.types._
import common.exceptions.UnsupportedOperation
import org.apache.spark
import org.apache.spark.sql.DataFrame
import schema._

/**
  * A [[PathPropertyGraph]] that uses Spark's [[DataFrame]]s to store graph data. Each tableName in the
  * data model of the graph is backed by a [[DataFrame]]. The [[GraphSchema]] is inferred from the
  * structure of each [[DataFrame]], so an extending class should typically define (in its
  * particular way) the [[schema.GraphData.vertexData]], [[schema.GraphData.edgeData]] and
  * [[schema.GraphData.pathData]].
  */
abstract class SparkGraph extends PathPropertyGraph {

  override type StorageType = DataFrame

  override def vertexSchema: EntitySchema = buildSchema(vertexData)

  override def edgeSchema: EntitySchema = buildSchema(edgeData)

  override def pathSchema: EntitySchema = buildSchema(pathData)

  def sparkSchemaString: String =
    s"\nGraph: $graphName\n" +
      s"[*] Vertex schema:\n$vertexData\n" +
      s"[*] Edge schema:\n$edgeData\n" +
      s"[*] Path schema:\n$pathData"

  /**
    * Infers the schema of an entity type (vertex, edge, path) from the sequence of data [[Table]]s
    * comprising this graph.
    */
  private def buildSchema(data: Seq[Table[DataFrame]]): EntitySchema =
    data.foldLeft(EntitySchema.empty) {
      case (aggSchema, table) =>
        val schemaFields = table.data.schema.fields
        val schemaMap = schemaFields.foldLeft(SchemaMap.empty[PropertyKey, GcoreDataType]) {
          case (aggMap, field) =>
            aggMap union SchemaMap(Map(PropertyKey(field.name) -> convertType(field.dataType)))
        }

        aggSchema union EntitySchema(SchemaMap(Map(table.name -> schemaMap)))
    }

  /** Maps a [[spark.sql.types.DataType]] to an algebraic [[GcoreDataType]]. */
  // TODO: Check that the array type can only be the sequence of edges that define a path.
  private def convertType(sparkType: spark.sql.types.DataType): GcoreDataType = {
    sparkType.typeName match {
      case "string" => GcoreString
      case "integer" => GcoreInteger
      case "long" => GcoreInteger
      case "double" => GcoreDecimal
      case "float" => GcoreDecimal
      case "boolean" => GcoreBoolean
      case "array" => GcoreArray
      case other => throw UnsupportedOperation(s"Unsupported type $other")
    }
  }
}
