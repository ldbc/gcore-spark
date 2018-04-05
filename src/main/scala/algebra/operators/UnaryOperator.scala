package algebra.operators

/**
  * A [[RelationalOperator]] that can be applied on one [[RelationLike]]. By default, the
  * [[BindingSet]] of the [[RelationLike]] becomes the [[BindingSet]] of the resulting
  * [[RelationLike]].
  */
abstract class UnaryOperator(relation: RelationLike, bindingSet: Option[BindingSet] = None)
  extends RelationLike(bindingSet.getOrElse(relation.getBindingSet)) {

  children = List(relation)
}
