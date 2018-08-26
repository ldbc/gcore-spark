package algebra.operators

/** A column in a relation. */
case class Column(columnName: String) extends RelationalOperator {

  override def name: String = s"${super.name} [$columnName]"
}

object Column {

  /**
    * Fixed column names we expect to find in physical tables, for specific entity types:
    * - Vertex, edge and path tables should contain the [[ID_COL]];
    * - Edge and path tables should contain the [[FROM_ID_COL]] and [[TO_ID_COL]];
    * - Path tables should contain the [[EDGE_SEQ_COL]].
    */
  val ID_COL: Column = Column("id")
  val FROM_ID_COL: Column = Column("fromId")
  val TO_ID_COL: Column = Column("toId")
  val EDGE_SEQ_COL: Column = Column("edges")

  /** Other fixed column names as needed by targets. */
  val TABLE_LABEL_COL: Column = Column("table_label")
  val CONSTRUCT_ID_COL: Column = Column("construct_id")
}
