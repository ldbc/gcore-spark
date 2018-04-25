package algebra.operators

import algebra.expressions.Reference

/** Adds the [[reference]] as a new attribute to the [[relation]]. */
case class AddColumn(reference: Reference, relation: RelationLike)
  extends UnaryOperator(relation, Some(relation.getBindingSet ++ new BindingSet(reference))) {

  children = List(reference, relation)
}
