package algebra.operators

import algebra.expressions.Reference
import common.compiler.Context

import scala.collection.mutable

/**
  * The set of bindings an [[AlgebraPrimitive]] knows of. Bindings represent names of variables
  * used in the G-CORE query.
  */
case class BindingSet(bindings: Set[Reference]) {

  def this(refs: Reference*) = this(refs.toSet)

  /**
    * Union this [[BindingSet]] with another [[BindingSet]]. Returns a new [[BindingSet]] containing
    * all the elements that are either in this [[BindingSet]] or in the argument.
    *
    * Note: Given this is a [[Set]], duplicate elements are eliminated under set union.
    */
  def ++(other: BindingSet): BindingSet = {
    BindingSet(bindings = bindings ++ other.bindings)
  }

  /**
    * @see [[++]]
    */
  def +=(other: BindingSet): BindingSet =
    this ++ other

  override def toString: String = s"{${bindings.mkString(", ")}}"

  /** Applies the given function on each element of this [[BindingSet]]. */
  def foreach(f: Reference => Unit): Unit =
    bindings.foreach(f)

  /**
    * Returns a new [[BindingSet]] containing the common elements between this [[BindingSet]] and
    * the argument.
    */
  def intersect(other: BindingSet): BindingSet =
    BindingSet(bindings.intersect(other.bindings))

  def size: Int = bindings.size

  def isEmpty: Boolean = this.size == 0

  def nonEmpty: Boolean = this.size != 0
}

object BindingSet {
  val empty: BindingSet = BindingSet(Set.empty)

  /**
    * Returns the intersection of all the given [[BindingSet]]s, i.e., returns all the bindings that
    * appear in at least two [[BindingSet]]s.
    */
  def intersect(bsets: Seq[BindingSet]): BindingSet = {
    val refToNumOccur: mutable.Map[Reference, Int] = mutable.Map()
    bsets.foreach(_.foreach(ref => {
      val ocurr: Int = {
        if (refToNumOccur.contains(ref))
          refToNumOccur(ref) + 1
        else
          1
      }
      refToNumOccur += (ref -> ocurr)
    }))

    BindingSet(
      refToNumOccur
        .filter(refToNumOccur => refToNumOccur._2 >= 2)
        .keys
        .toSet)
  }
}


/** A [[Context]] wrapping a [[BindingSet]] within an [[AlgebraPrimitive]]. */
case class BindingContext(bset: BindingSet) extends Context {

  def this(refs: Reference*) = this(new BindingSet(refs: _*))

  // TODO: Can we do something smarter here?
  def getOnlyBinding: Reference = bset.bindings.head

  /**
    * Returns a new [[BindingContext]] containing the union between the [[BindingSet]] within this
    * context and the [[BindingSet]] within the argument.
    *
    * Note: Given this is a [[Set]] union, duplicate elements are eliminated.
    */
  def ++(other: BindingContext): BindingContext = {
    BindingContext(bset ++ other.bset)
  }

  def isEmpty: Boolean = bset.isEmpty

  def nonEmpty: Boolean = bset.nonEmpty

  override def toString: String = s"$bset"
}

object BindingContext {
  val empty = new BindingContext()
}
