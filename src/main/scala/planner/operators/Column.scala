package planner.operators

import planner.trees.PlannerTreeNode

case class Column(columnName: String) extends PlannerTreeNode {

  override def name: String = s"${super.name} [$columnName]"
}

object Column {
  val idColumn: Column = Column("id")
  val fromIdColumn: Column = Column("fromId")
  val toIdColumn: Column = Column("toId")

  val tableLabelColumn: Column = Column("table_label")
}
