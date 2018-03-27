package algebra.operators

abstract class AlgebraPrimitive extends AlgebraOperator

/**
  * An [[AlgebraPrimitive]] that can be applied on one [[RelationLike]]. By default, the
  * [[BindingSet]] of the [[RelationLike]] becomes the [[BindingSet]] of the resulting
  * [[RelationLike]].
  */
abstract class UnaryPrimitive(relation: RelationLike, bindingSet: Option[BindingSet] = None)
  extends RelationLike(bindingSet.getOrElse(relation.getBindingSet)) {

  children = List(relation)
}


/**
  * An [[AlgebraPrimitive]] that can be applied on two [[RelationLike]]s. By default, the
  * [[BindingSet]] of the resulting [[RelationLike]] becomes the union of the
  * [[BindingSet]]s of the two operands.
  */
abstract class BinaryPrimitive(lhs: RelationLike,
                               rhs: RelationLike,
                               bindingSet: Option[BindingSet] = None)
  extends RelationLike(bindingSet.getOrElse(lhs.getBindingSet ++ rhs.getBindingSet)) {

  children = List(lhs, rhs)
}

object BinaryPrimitive {

  /**
    * Reduces a sequence of [[BinaryPrimitive]]s, from left to right. The reduced result will be:
    *
    * reduce([R1, R2, R3, ... Rn], binaryOp) =
    *   binaryOp(
    *     binaryOp(R1, R2),
    *     reduce([R3, R4, ... Rn], binaryOp) = ...
    */
  def reduceLeft(relations: Seq[RelationLike],
                 binaryOp: (RelationLike, RelationLike, Option[BindingSet]) => RelationLike)
  : RelationLike = {

    relations match {
      case Seq() => RelationLike.empty
      case _ => relations.reduceLeft((agg, rel) => binaryOp(agg, rel, None))
    }
  }
}
