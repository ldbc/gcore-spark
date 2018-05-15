package algebra.target_api

import algebra.expressions.Reference
import algebra.operators.{EntityCreateRule, RemoveClause}

case class VertexCreate(reference: Reference,
                        removeClause: Option[RemoveClause],
                        tableBaseIndex: Int) extends EntityCreateRule {

  children = List(reference) ++ removeClause.toList

  override def name: String = s"${super.name} [${reference.refName}, $tableBaseIndex]"
}
