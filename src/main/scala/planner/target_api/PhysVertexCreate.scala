package planner.target_api

import algebra.expressions.{ObjectConstructPattern, Reference}
import algebra.operators.{RemoveClause, SetClause}
import planner.trees.TargetTreeNode

abstract class PhysVertexCreate(reference: Reference,
                                relation: TargetTreeNode,
                                expr: ObjectConstructPattern,
                                setClause: Option[SetClause],
                                removeClause: Option[RemoveClause]) extends TargetTreeNode {
  children = List(reference, relation, expr) ++ setClause.toList ++ removeClause.toList
}
