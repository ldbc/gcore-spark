package schema

import org.apache.spark.sql.DataFrame

/**
  * Data contained by a graph.
  * @tparam T The type of the collection that backs the actual data. Depending on the system used
  *           as a backend, this type may differ. For example, for a Spark solution, we could
  *           represent a table as a [[DataFrame]].
  */
trait GraphData[T] {

  def vertexData: Seq[Table[T]]

  def edgeData: Seq[Table[T]]

  def pathData: Seq[Table[T]]

  def tableMap: Map[String, Table[T]] =
    (vertexData map {table => table.name -> table}).toMap ++
      (edgeData map {table => table.name -> table}).toMap ++
      (pathData map {table => table.name -> table}).toMap
}

/**
  * A logical table backing the data.
  * @tparam T The type of the collection that stores the physical data.
  */
case class Table[T](name: String, data: T) {

  override def toString: String = s"Table $name: $data"
}
