package algebra.operators

import algebra.expressions.Label

/**
  * A logical table in the algebraic tree. It is the binding table produced by any
  * [[RelationalOperator]]. All [[RelationLike]]s have a [[BindingSet]] that represents the header
  * of the table.
  */
abstract class RelationLike(bindingSet: BindingSet) extends RelationalOperator {

  def getBindingSet: BindingSet = bindingSet

  override def name: String = s"${super.name} [bindingSet = $bindingSet]"
}

object RelationLike {

  /** The empty binding table. */
  val empty: RelationLike = new RelationLike(BindingSet.empty) {
    override def name: String = "EmptyRelation"
  }
}

/**
  * A wrapper over a [[Label]], such that we can use the [[Label]] (which is effectively a table in
  * the database) in the relational tree.
  */
case class Relation(label: Label) extends RelationLike(BindingSet.empty) {
  children = List(label)
}

/**
  * Given that not all variables are labeled upfront in the query, they cannot be assigned a strict
  * [[Relation]] and instead can be any relation in the database.
  */
case object AllRelations extends RelationLike(BindingSet.empty)
