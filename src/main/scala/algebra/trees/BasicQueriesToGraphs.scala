package algebra.trees

import algebra.operators.{GraphCreate, GraphUnion, Query}
import common.exceptions.UnsupportedOperation
import common.trees.TopDownRewriter

object BasicQueriesToGraphs extends TopDownRewriter[AlgebraTreeNode] {

  override val rule: BasicQueriesToGraphs.RewriteFuncType = {
    case q: Query =>
      GraphCreate(
        matchClause = q.getMatchClause,
        groupConstructs = q.getConstructClause.children)
  }
}
