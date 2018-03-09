package algebra.operators

import algebra.expressions.Label
import algebra.types.Graph
import common.compiler.Context

abstract class RelationLike(bindingContext: BindingContext)
  extends AlgebraPrimitive {

  def getBindingContext: BindingContext = bindingContext

  def getBindings: BindingSet = bindingContext.bset

  override def name: String = s"${super.name} [bindingSet = $bindingContext]"
}

object RelationLike {
  val empty: RelationLike = new RelationLike(BindingContext.empty) {
    override def name: String = "EmptyRelation"
  }
}

case class Relation(rel: Label, bindingContext: Option[BindingContext] = None)
  extends RelationLike(bindingContext.getOrElse(BindingContext.empty)) {

  children = List(rel)
}

case class SimpleMatchRelationContext(graph: Graph) extends Context {

  override def toString: String = s"$graph"
}

case class SimpleMatchRelation(relation: RelationLike,
                               context: SimpleMatchRelationContext,
                               bindingContext: Option[BindingContext] = None)
  extends UnaryPrimitive(relation, bindingContext) {

  override def name: String = s"${super.name} [graph = ${context.graph}]"
}
