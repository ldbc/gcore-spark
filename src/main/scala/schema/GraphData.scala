package schema

import algebra.expressions.Label

/** Data contained by a graph. */
trait GraphData {

  type StorageType

  def vertexData: Seq[Table[StorageType]]

  def edgeData: Seq[Table[StorageType]]

  def pathData: Seq[Table[StorageType]]

  def tableMap: Map[Label, Table[StorageType]] =
    (vertexData map {table => table.name -> table}).toMap ++
      (edgeData map {table => table.name -> table}).toMap ++
      (pathData map {table => table.name -> table}).toMap
}

/**
  * A physical tableName backing the data.
  * @tparam StorageType The type of the collection that stores the physical data.
  */
case class Table[StorageType](name: Label, data: StorageType) {
  override def toString: String = s"Table $name: $data"
}
