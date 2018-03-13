package algebra.operators

import algebra.expressions.{Label, Reference}
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

abstract class EntityRelation(ref: Reference,
                              relation: RelationLike,
                              bindingTable: BindingTable) extends RelationLike(bindingTable) {
  children = List(ref, relation)
}

case class VertexRelation(ref: Reference,
                          relation: RelationLike,
                          bindingTable: BindingTable)
  extends EntityRelation(ref, relation, bindingTable)

case class EdgeRelation(ref: Reference,
                        relation: RelationLike,
                        bindingTable: BindingTable)
  extends EntityRelation(ref, relation, bindingTable)

case class SimpleMatchRelationContext(graph: Graph) extends Context {

  override def toString: String = s"$graph"
}

case class SimpleMatchRelation(relation: RelationLike,
                               context: SimpleMatchRelationContext,
                               bindingTable: Option[BindingTable] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  override def name: String = s"${super.name} [graph = ${context.graph}]"
}
