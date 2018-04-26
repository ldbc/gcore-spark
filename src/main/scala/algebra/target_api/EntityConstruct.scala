package algebra.target_api

import algebra.expressions.{ObjectConstructPattern, PropertyRef, Reference}
import algebra.operators.{RemoveClause, SetClause}

abstract class EntityConstruct(reference: Reference,
                               isMatchedRef: Boolean,
                               relation: TargetTreeNode,
                               groupedAttributes: Seq[PropertyRef],
                               expr: ObjectConstructPattern,
                               setClause: Option[SetClause],
                               removeClause: Option[RemoveClause]) extends TargetTreeNode {
  children = List(reference, relation, expr) ++ groupedAttributes ++
    setClause.toList ++ removeClause.toList
}
