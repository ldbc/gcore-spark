package algebra.operators

import algebra.expressions.{ObjectPattern, Reference}
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

case class EntityRelation(ref: Reference, objPattern: ObjectPattern)
  extends RelationLike(new BindingContext(ref)) {

  children = List(ref, objPattern)
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
