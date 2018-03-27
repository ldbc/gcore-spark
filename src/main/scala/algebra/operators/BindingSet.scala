package algebra.operators

import algebra.expressions.Reference

import scala.collection.mutable

case class BindingSet(refSet: Set[Reference]) {

  def this(refs: Reference*) =
    this(Set(refs: _*))

  def ++(other: BindingSet): BindingSet = {
    BindingSet(this.refSet ++ other.refSet)
  }

  override def toString: String = s"{${refSet.mkString(", ")}}"
}

object BindingSet {

  val empty: BindingSet = newEmpty

  def newEmpty: BindingSet = BindingSet(Set.empty)

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

