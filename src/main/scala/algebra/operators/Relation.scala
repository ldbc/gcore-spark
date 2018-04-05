package algebra.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.types.{Edge, Graph, PathQuantifier, Path, Vertex}
import common.compiler.Context

/**
  * A logical table in the algebraic tree. It is the binding table produced by any
  * [[RelationalOperator]]. All [[RelationLike]]s have a [[BindingSet]] that represents the header
  * the table.
  */
abstract class RelationLike(bindingSet: BindingSet) extends RelationalOperator {

  def getBindingSet: BindingSet = bindingSet

  override def name: String =
    s"${super.name} [bindingSet = $bindingSet]"
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
case class AllRelations() extends RelationLike(BindingSet.empty)

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
                              quantifier: Option[PathQuantifier])
  extends RelationLike(new BindingSet(ref) ++ fromRel.getBindingSet ++ toRel.getBindingSet) {

  children = List(ref, labelRelation, expr, fromRel, toRel)
}

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
