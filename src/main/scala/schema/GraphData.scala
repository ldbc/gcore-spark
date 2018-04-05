package schema

import algebra.expressions.Label

/** Data stored in a graph. */
trait GraphData {

  /** Backend-specific storage type for graph data. */
  type StorageType

  def vertexData: Seq[Table[StorageType]]

  def edgeData: Seq[Table[StorageType]]

  def pathData: Seq[Table[StorageType]]

  /**
    * A mapping from table name to table data. Given that each [[Label]] is a table, we express the
    * name through that particular [[Label]].
    */
  def tableMap: Map[Label, Table[StorageType]] =
    (vertexData map {table => table.name -> table}).toMap ++
      (edgeData map {table => table.name -> table}).toMap ++
      (pathData map {table => table.name -> table}).toMap
}

/** A physical table backing the data. */
case class Table[StorageType](name: Label, data: StorageType) {
  override def toString: String = s"Table $name: $data"
}
