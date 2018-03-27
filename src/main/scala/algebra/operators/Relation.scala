package algebra.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.types.{Graph, PathQuantifier}
import common.compiler.Context

abstract class RelationLike(bindingSet: BindingSet) extends AlgebraPrimitive {

  def getBindingSet: BindingSet = bindingSet

  override def name: String =
    s"${super.name} [bindingSet = $bindingSet]"
}

object RelationLike {
  val empty: RelationLike = new RelationLike(BindingSet.empty) {
    override def name: String = "EmptyRelation"
  }
}

case class Relation(label: Label) extends RelationLike(BindingSet.empty) {
  children = List(label)
}

case class AllRelations() extends RelationLike(BindingSet.empty)

case class VertexRelation(ref: Reference,
                          labelRelation: RelationLike,
                          expr: AlgebraExpression) extends RelationLike(new BindingSet(ref)) {
  children = List(ref, labelRelation, expr)
}

case class EdgeRelation(ref: Reference,
                        labelRelation: RelationLike,
                        expr: AlgebraExpression,
                        fromRel: VertexRelation,
                        toRel: VertexRelation)
  extends RelationLike(new BindingSet(ref) ++ fromRel.getBindingSet ++ toRel.getBindingSet) {

  children = List(ref, labelRelation, expr, fromRel, toRel)
}

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

case class SimpleMatchRelation(relation: RelationLike,
                               context: SimpleMatchRelationContext,
                               bindingSet: Option[BindingSet] = None)
  extends UnaryPrimitive(relation, bindingSet) {

  override def name: String = s"${super.name} [graph = ${context.graph}]"
}

case class CondMatchRelation(relation: RelationLike,
                             expr: AlgebraExpression,
                             bindingSet: Option[BindingSet] = None)
  extends UnaryPrimitive(relation, bindingSet) {

  children = List(relation, expr)
}
