package schema

/** Data contained by a graph. */
trait GraphData {

  type T

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
