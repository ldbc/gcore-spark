package algebra.operators

import common.compiler.Context

case class Query(matchClause: MatchClause) extends GcorePrimitive {

  children = List(matchClause)

  override def checkWithContext(context: Context): Unit =
    matchClause.checkWithContext(context)
}
