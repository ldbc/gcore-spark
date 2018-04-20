package algebra.operators

import algebra.expressions.Reference

/** Projects the given [[attributes]] from the [[relation]]. */
case class Project(relation: RelationLike,
                   attributes: Set[Reference])
  extends UnaryOperator(relation, bindingSet = Some(BindingSet(attributes))) {

  children = relation +: attributes.toSeq
}
