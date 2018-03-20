package algebra.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.types.Graph
import common.compiler.Context

abstract class RelationLike(bindingTable: BindingSet) extends AlgebraPrimitive {

  def getBindingTable: BindingSet = bindingTable

  override def name: String =
    s"${super.name} [bindingTable = $bindingTable]"
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
  extends RelationLike(new BindingSet(ref) ++ fromRel.getBindingTable ++ toRel.getBindingTable) {

  children = List(ref, labelRelation, expr, fromRel, toRel)
}

case class SimpleMatchRelationContext(graph: Graph) extends Context {

  override def toString: String = s"$graph"
}

case class SimpleMatchRelation(relation: RelationLike,
                               context: SimpleMatchRelationContext,
                               bindingTable: Option[BindingSet] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  override def name: String = s"${super.name} [graph = ${context.graph}]"
}

case class CondMatchRelation(relation: RelationLike,
                             expr: AlgebraExpression,
                             bindingTable: Option[BindingSet] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  children = List(relation, expr)
}
