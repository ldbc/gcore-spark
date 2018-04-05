package algebra.operators

import algebra.expressions.Reference

import scala.collection.mutable

/**
  * The set of variables (bindings) that a [[RelationLike]] knows of. Can be viewed as the header of
  * the binding table.
  *
  * For example, if a root [[RelationLike]] is applied over one [[RelationLike]] with binding set
  * {u, v} and another with binding set {w}, the binding set at the root will be {u, v, w} and the
  * binding table's header is:
  *
  * +---+---+---+
  * | u | v | w |
  * +---+---+---+
  * |.. |.. |.. |
  *
  */
case class BindingSet(refSet: Set[Reference]) {

  def this(refs: Reference*) =
    this(Set(refs: _*))

  /**
    * Creates a new [[BindingSet]] containing all the bindings in this set and all the bindings in
    * other. Given this is a <i>set</i> union, each binding on both sides of the operation will
    * appear only once in the result.
    */
  def ++(other: BindingSet): BindingSet = {
    BindingSet(this.refSet ++ other.refSet)
  }

  override def toString: String = s"{${refSet.mkString(", ")}}"
}

object BindingSet {

  val empty: BindingSet = newEmpty

  def newEmpty: BindingSet = BindingSet(Set.empty)

  /**
    * Creates a set containing all the [[Reference]]s that appear in at least two of the given sets.
    */
  def intersectBindingSets(btables: Seq[Set[Reference]]): Set[Reference] = {
    val refToNumOccur: mutable.Map[Reference, Int] = mutable.Map()
    btables.foreach(_.foreach(ref => {
      val ocurr: Int = {
        if (refToNumOccur.contains(ref))
          refToNumOccur(ref) + 1
        else
          1
      }
      refToNumOccur += (ref -> ocurr)
    }))

    refToNumOccur
      .filter(refToNumOccur => refToNumOccur._2 >= 2)
      .keys
      .toSet
  }
}

