package algebra.operators

import algebra.trees.AlgebraTreeNode
import common.compiler.Context

/**
  * The root of the algebraic tree between the parsing and full conversion into a relational tree.
  */
case class Query(constructClause: ConstructClause, matchClause: MatchClause) extends GcoreOperator {

  children = List(constructClause, matchClause)

  def getConstructClause: AlgebraTreeNode = children.head
  def getMatchClause: AlgebraTreeNode = children.last

  override def checkWithContext(context: Context): Unit = {
    constructClause.checkWithContext(context)
    matchClause.checkWithContext(context)
  }
}
