package algebra.operators

import algebra.expressions.{AggregateExpression, AlgebraExpression, PropertySet, Reference}
import algebra.trees.AlgebraTreeNode

/**
  * The relational group by operation. The [[aggregateFunctions]] will create new properties for the
  * [[reference]] by using [[AlgebraExpression]]s over [[AggregateExpression]]s.
  *
  * For example, the following patterns will result in the respective parameters:
  * > CONSTRUCT (c) => [[reference]] = c
  *                    [[groupingAttributes]] = {c} // group by identity
  *                    [[aggregateFunctions]] = {}
  *
  * > CONSTRUCT (x) => [[reference]] = x
  *                    [[groupingAttributes]] = {} // nothing to group by, btable used as is
  *                    [[aggregateFunctions]] = {}
  *
  * > CONSTRUCT (x GROUP c.prop) => [[reference]] = x
  *                                 [[groupingAttributes]] = {c.prop}
  *                                 [[aggregateFunctions]] = {}
  *
  * > CONSTRUCT (x GROUP c.prop0 {prop1 := AVG(c.prop1)}) =>
  *                                 [[reference]] = x
  *                                 [[groupingAttributes]] = {c.prop}
  *                                 [[aggregateFunctions]] = {x.prop1 := AVG(c.prop1)}
  */
case class GroupBy(reference: Reference, // The entity for which we group
                   relation: RelationLike,
                   groupingAttributes: Seq[AlgebraTreeNode], // Reference or GroupDeclaration
                   aggregateFunctions: Seq[PropertySet],
                   having: Option[AlgebraExpression] = None)
  extends UnaryOperator(relation, bindingSet = Some(new BindingSet(reference))) {

  children = (reference +: relation +: groupingAttributes) ++ aggregateFunctions ++ having.toList

  /** Handy extractors of this operator's children. */
  def getRelation: AlgebraTreeNode = children(1)

  def getGroupingAttributes: Seq[AlgebraTreeNode] = children.slice(2, 2 + groupingAttributes.size)

  def getAggregateFunction: Seq[PropertySet] = aggregateFunctions

  def getHaving: Option[AlgebraExpression] = having
}
