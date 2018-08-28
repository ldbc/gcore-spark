package algebra.operators

import algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW

/**
  * An algebraic sub-tree with a name. Can be reused within other sub-trees, instead of the original
  * algebraic plan that it references.
  */
abstract class TableView(viewName: String, bindingSet: BindingSet)
  extends RelationLike(bindingSet) {

  override def name: String = s"${super.name} [$viewName]"

  def getViewName: String = viewName
}

/**
  * Placeholder for the binding table materialized from the MATCH clause. During the rewriting phase
  * of the algebraic tree, we don't have access to the actual matched data.
  */
case class BindingTableView(bindingSet: BindingSet) extends TableView(BTABLE_VIEW, bindingSet)

/**
  * Placeholder for the filtered binding table used in constructing the entities in a
  * [[GroupConstruct]].
  */
case class BaseConstructTableView(viewName: String, bindingSet: BindingSet)
  extends TableView(viewName, bindingSet)

/**
  * Placeholder for the table resulting after the construction of the vertices in a
  * [[GroupConstruct]].
  */
case class VertexConstructTableView(viewName: String, bindingSet: BindingSet)
  extends TableView(viewName, bindingSet)

case class ConstructRelationTableView(viewName: String, bindingSet: BindingSet)
  extends TableView(viewName, bindingSet)
