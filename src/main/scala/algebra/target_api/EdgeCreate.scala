package algebra.target_api

import algebra.expressions.Reference
import algebra.operators.{EntityCreateRule, RemoveClause}
import algebra.types.ConnectionType

case class EdgeCreate(reference: Reference,
                      leftReference: Reference,
                      rightReference: Reference,
                      connType: ConnectionType,
                      removeClause: Option[RemoveClause],
                      tableBaseIndex: Int) extends EntityCreateRule {

  children = List(reference, leftReference, rightReference, connType) ++ removeClause.toList

  override def name: String =
    s"${super.name} " +
      s"[${reference.refName}, ${leftReference.refName}, ${rightReference.refName}, $connType, " +
      s"$tableBaseIndex]"
}
