package algebra.trees

import algebra.operators.{GraphCreate, GraphUnion, Query}
import common.exceptions.UnsupportedOperation
import common.trees.TopDownRewriter

object BasicQueriesToGraphs extends TopDownRewriter[AlgebraTreeNode] {

  override val rule: BasicQueriesToGraphs.RewriteFuncType = {
    case q: Query =>
      val graphUnion = q.getConstructClause.children.head.asInstanceOf[GraphUnion]
      if (graphUnion.graphs.nonEmpty)
        throw UnsupportedOperation("Graph union is not supported in CONSTRUCT clause.")

      val condConstruct = q.getConstructClause.children(1)
      GraphCreate(
        matchClause = q.getMatchClause,
        constructClauses = condConstruct.children)
  }
}
