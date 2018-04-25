package planner.operators

import algebra.expressions.{ObjectConstructPattern, PropertyRef, Reference}
import algebra.operators.{RemoveClause, SetClause}
import planner.trees.PlannerTreeNode


case class EntityConstruct(reference: Reference,
                           isMatchedRef: Boolean,
                           bindingTable: PlannerTreeNode,
                           groupedAttributes: Seq[PropertyRef],
                           expr: ObjectConstructPattern,
                           setClause: Option[SetClause],
                           removeClause: Option[RemoveClause]) extends PlannerTreeNode {
  children = List(reference, bindingTable, expr) ++ groupedAttributes ++
    setClause.toList ++ removeClause.toList
}
