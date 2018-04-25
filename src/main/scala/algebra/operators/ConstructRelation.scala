package algebra.operators

import algebra.expressions.{ObjectConstructPattern, PropertyRef, Reference}
import algebra.types.ConnectionType
import schema.PathPropertyGraph

/**
  * A relation that holds the data of a new graph entity, denoted by its [[reference]]. This data
  * can then be used to add the respective entity to its corresponding [[PathPropertyGraph]].
  */
abstract class ConstructRelation(reference: Reference,
                                 relation: RelationLike) extends UnaryOperator(relation) {
  def getReference: Reference = reference
}

/**
  * The [[ConstructRelation]] of an entity in the graph. Dictates how the entity should be built
  * starting from the binding table. New properties can be added to the entity through its
  * [[ObjectConstructPattern]] or through the [[SetClause]]. Labels can be added through the
  * [[ObjectConstructPattern]]. The [[RemoveClause]] can be used to remove properties and/or labels.
  * The [[PropertyRef]]erences used for grouping the binding table (if there were any) are exposed
  * through the [[groupedAttributes]] parameter.
  */
case class EntityConstructRelation(reference: Reference,
                                   isMatchedRef: Boolean,
                                   relation: RelationLike,
                                   groupedAttributes: Seq[PropertyRef] = Seq.empty,
                                   expr: ObjectConstructPattern,
                                   setClause: Option[SetClause],
                                   removeClause: Option[RemoveClause])
  extends ConstructRelation(reference, relation) {

  children = List(reference, relation, expr) ++ groupedAttributes ++
    setClause.toList ++ removeClause.toList
}

/**
  * The [[ConstructRelation]] of a vertex - not only simple vertices, but also connection endpoints.
  */
case class VertexConstructRelation(reference: Reference,
                                   relation: RelationLike)
  extends ConstructRelation(reference, relation)

/** The [[ConstructRelation]] of an edge. */
case class EdgeConstructRelation(reference: Reference,
                                 relation: RelationLike,
                                 leftReference: Reference,
                                 rightReference: Reference,
                                 connType: ConnectionType)
  extends ConstructRelation(reference, relation) {

  children = List(reference, relation, leftReference, rightReference)
}

