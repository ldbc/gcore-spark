package algebra.operators

/**
  * A [[RelationalOperator]] between two [[RelationLike]]s. By default, the [[BindingSet]] of the
  * resulting [[RelationLike]] becomes the union of the [[BindingSet]]s of the two operands.
  */
abstract class BinaryOperator(lhs: RelationLike,
                              rhs: RelationLike,
                              bindingSet: Option[BindingSet] = None)
  extends RelationLike(bindingSet.getOrElse(lhs.getBindingSet ++ rhs.getBindingSet)) {

  children = List(lhs, rhs)
}

object BinaryOperator {

  /**
    * Reduces a sequence of [[BinaryOperator]]s, from left to right. The reduced result will be:
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
