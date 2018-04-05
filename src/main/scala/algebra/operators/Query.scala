package algebra.operators

import common.compiler.Context

/**
  * The root of the algebraic tree between the parsing and full conversion into a relational tree.
  */
case class Query(matchClause: MatchClause) extends GcoreOperator {

  children = List(matchClause)

  override def checkWithContext(context: Context): Unit =
    matchClause.checkWithContext(context)
}
