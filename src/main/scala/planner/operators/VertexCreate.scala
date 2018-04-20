package planner.operators

import algebra.expressions.{ObjectConstructPattern, Reference}
import algebra.operators.{RemoveClause, SetClause}
import planner.trees.PlannerTreeNode

case class VertexCreate(reference: Reference,
                        bindingTable: PlannerTreeNode,
                        expr: ObjectConstructPattern,
                        setClause: Option[SetClause],
                        removeClause: Option[RemoveClause]) extends PlannerTreeNode {
  children = List(reference, bindingTable, expr) ++ setClause.toList ++ removeClause.toList

  def getSetClause: Option[SetClause] = {
    if (setClause.isDefined)
      Some(children(3).asInstanceOf[SetClause])
    else None
  }

  def getRemoveClause: Option[RemoveClause] = {
    if (removeClause.isDefined)
      if (setClause.isDefined)
        Some(children(4).asInstanceOf[RemoveClause])
      else
        Some(children(3).asInstanceOf[RemoveClause])
    else None
  }
}
