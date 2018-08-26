package algebra.operators

import algebra.expressions.{AlgebraExpression, Reference}
import algebra.types._
import common.compiler.Context

case class SimpleMatchRelationContext(graph: Graph) extends Context {

  override def toString: String = s"$graph"
}

/**
  * A [[RelationLike]] over a [[VertexRelation]], an [[EdgeRelation]] or a [[StoredPathRelation]]
  * that additionally knows in which [[Graph]] to look for the entity's table. It is essentially a
  * wrapper over the G-CORE specific [[SimpleMatchClause]] such that we can use it in the relational
  * tree.
  */
case class SimpleMatchRelation(relation: RelationLike,
                               context: SimpleMatchRelationContext,
                               bindingSet: Option[BindingSet] = None)
  extends UnaryOperator(relation, bindingSet) {

  override def name: String = s"${super.name} [graph = ${context.graph}]"
}

/**
  * The logical table that contains data for a vertex. It is essentially a wrapper over the G-CORE-
  * specific [[Vertex]] such that we can use the entity in the relational tree.
  */
case class VertexRelation(ref: Reference,
                          labelRelation: RelationLike,
                          expr: AlgebraExpression) extends RelationLike(new BindingSet(ref)) {
  children = List(ref, labelRelation, expr)
}

/**
  * The logical table that contains data for an edge. It is essentially a wrapper over the G-CORE-
  * specific [[Edge]] such that we can use the entity in the relational tree.
  */
case class EdgeRelation(ref: Reference,
                        labelRelation: RelationLike,
                        expr: AlgebraExpression,
                        fromRel: VertexRelation,
                        toRel: VertexRelation)
  extends RelationLike(new BindingSet(ref) ++ fromRel.getBindingSet ++ toRel.getBindingSet) {

  children = List(ref, labelRelation, expr, fromRel, toRel)
}

/**
  * The logical table that contains data for a stored path. It is essentially a wrapper over the
  * G-CORE-specific [[Path]] with parameter [[Path.isObj]] = true, such that we can use the entity
  * in the relational tree.
  */
case class StoredPathRelation(ref: Reference,
                              isReachableTest: Boolean,
                              labelRelation: RelationLike,
                              expr: AlgebraExpression,
                              fromRel: VertexRelation,
                              toRel: VertexRelation,
                              costVarDef: Option[Reference],
                              quantifier: PathQuantifier)
  extends RelationLike(new BindingSet(ref) ++ fromRel.getBindingSet ++ toRel.getBindingSet) {

  children = List(ref, labelRelation, expr, fromRel, toRel)
}

/**
  * The logical table that contains data for a virtual path (a path that we need to discover in the
  * graph). It is essentially a wrapper over the G-CORE-specific [[Path]] with parameter
  * [[Path.isObj]] = true, such that we can use the entity in the relational tree.
  */
case class VirtualPathRelation(ref: Reference,
                               isReachableTest: Boolean,
                               fromRel: VertexRelation,
                               toRel: VertexRelation,
                               costVarDef: Option[Reference],
                               pathExpression: Option[PathExpression])
  extends RelationLike(new BindingSet(ref) ++ fromRel.getBindingSet ++ toRel.getBindingSet) {

  children = List(ref, fromRel, toRel)
}
