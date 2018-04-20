package algebra.operators

import algebra.expressions.{ObjectConstructPattern, Reference}

/**
  * A construction relation that dictates how a vertex should be built starting from the
  * [[BindingTable]]. New properties can be added to the entity through its
  * [[ObjectConstructPattern]] or through the [[SetClause]]. Labels can be added through the
  * [[ObjectConstructPattern]]. The [[RemoveClause]] can be used to remove properties and/or labels
  * The [[relation]] should be an algebraic relational tree over the [[BindingTable]].
  */
case class VertexConstructRelation(reference: Reference,
                                   relation: RelationLike,
                                   expr: ObjectConstructPattern,
                                   setClause: Option[SetClause],
                                   removeClause: Option[RemoveClause])
  extends UnaryOperator(relation) {

  children = List(reference, relation, expr) ++ setClause.toList ++ removeClause.toList
}
