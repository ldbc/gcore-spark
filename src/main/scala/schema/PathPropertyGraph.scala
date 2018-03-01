package schema

import org.apache.spark.sql.DataFrame

/**
  * The path property graph is the queryable unit of G-CORE. The graph retains information about
  * vertices, edges and also paths. The stored information refers to labels and key-value
  * attributes of an entity. A graph has a [[GraphSchema]] that describes the structure and
  * properties the stored [[GraphData]].
  *
  * @tparam T The type of the collection that backs the actual data. Depending on the system used
  *           as a backend, this type may differ. For example, for a Spark solution, we could
  *           represent a table as a [[DataFrame]].
  */
abstract class PathPropertyGraph[T] extends GraphSchema with GraphData[T] {

  def graphName: String

  override def toString: String = schemaString

  def schemaString: String =
    s"\nGraph: $graphName\n" +
      s"[*] Vertex schema:\n$vertexSchema" +
      s"[*] Edge schema:\n$edgeSchema" +
      s"[*] Path schema:\n$pathSchema"

  def sparkSchemaString: String =
    s"\nGraph: $graphName\n" +
      s"[*] Vertex schema:\n$vertexData\n" +
      s"[*] Edge schema:\n$edgeData\n" +
      s"[*] Path schema:\n$pathData"
}
