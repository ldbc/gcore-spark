package algebra.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.types.Graph
import common.compiler.Context

abstract class RelationLike(bindingTable: BindingTable) extends AlgebraPrimitive {

  def getBindingTable: BindingTable = bindingTable

  override def name: String =
    s"${super.name} [bindingSet = ${bindingTable.bindingSet.mkString(", ")}]"
}

object RelationLike {
  val empty: RelationLike = new RelationLike(BindingTable.empty) {
    override def name: String = "EmptyRelation"
  }
}

case class Relation(label: Label) extends RelationLike(BindingTable.empty) {
  children = List(label)
}

case class AllRelations() extends RelationLike(BindingTable.empty)

case class VertexRelation(ref: Reference,
                          labelRelation: RelationLike,
                          expr: AlgebraExpression) extends RelationLike(new BindingTable(ref)) {
  children = List(ref, labelRelation, expr)
}

case class EdgeRelation(ref: Reference,
                        labelRelation: RelationLike,
                        expr: AlgebraExpression,
                        fromRel: VertexRelation,
                        toRel: VertexRelation)
  extends RelationLike(new BindingTable(ref) ++ fromRel.getBindingTable ++ toRel.getBindingTable) {

  children = List(ref, labelRelation, expr, fromRel, toRel)
}

case class SimpleMatchRelationContext(graph: Graph) extends Context {

  override def toString: String = s"$graph"
}

case class SimpleMatchRelation(relation: RelationLike,
                               context: SimpleMatchRelationContext,
                               bindingTable: Option[BindingTable] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  override def name: String = s"${super.name} [graph = ${context.graph}]"
}
