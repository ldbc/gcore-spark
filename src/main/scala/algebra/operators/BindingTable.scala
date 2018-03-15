package algebra.operators

import algebra.expressions.Reference
import algebra.operators.BindingTable.BtableMmap

import scala.collection.mutable

case class BindingTable(btable: BtableMmap) {

  def this(refs: Reference*) =
    this(
      BindingTable.newEmpty.btable ++=
      refs.map(ref => (ref, mutable.Set(RelationLike.empty))))

  def this(refsToRel: Map[Reference, RelationLike]) =
    this(
      BindingTable.newEmpty.btable ++=
        refsToRel.map(refToRel => (refToRel._1, mutable.Set(refToRel._2))))

  def ++(other: BindingTable): BindingTable = {
    BindingTable((this.btable ++ other.btable).asInstanceOf[BtableMmap])
  }

  def relations(ref: Reference): Set[RelationLike] =
    btable.getOrElse(ref, Set(RelationLike.empty)).toSet

  def bindingSet: Set[Reference] = btable.keys.toSet

  override def toString: String = btable.toString()
}

object BindingTable {
  type BtableMmap = mutable.HashMap[Reference, mutable.Set[RelationLike]]
    with mutable.MultiMap[Reference, RelationLike]

  val empty: BindingTable = newEmpty

  def newEmpty: BindingTable =
    BindingTable(new mutable.HashMap[Reference, mutable.Set[RelationLike]]
      with mutable.MultiMap[Reference, RelationLike])

  def intersectBindingSets(bsets: Seq[Set[Reference]]): Set[Reference] = {
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

    refToNumOccur
      .filter(refToNumOccur => refToNumOccur._2 >= 2)
      .keys
      .toSet
  }
}

