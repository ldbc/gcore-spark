package algebra.target_api

/** Table view in the target tree. */
abstract class TableView(viewName: String) extends TargetTreeNode {

  override def name: String = s"${super.name} [$viewName]"
}
