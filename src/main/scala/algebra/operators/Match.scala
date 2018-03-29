package algebra.operators

import algebra.exceptions.{DefaultGraphNotAvailableException, NamedGraphNotAvailableException, UnsupportedOperation}
import algebra.expressions.AlgebraExpression
import algebra.trees.{GraphPatternContext, QueryContext}
import algebra.types._
import common.compiler.Context

abstract class MatchLike extends GcorePrimitive

case class MatchClause(nonOptMatches: CondMatchClause, optMatches: Seq[CondMatchClause])
  extends MatchLike {

  children = nonOptMatches +: optMatches

  override def checkWithContext(context: Context): Unit = {
    nonOptMatches.checkWithContext(context)
    optMatches.foreach(_.checkWithContext(context))
  }
}

case class CondMatchClause(simpleMatches: Seq[SimpleMatchClause], where: AlgebraExpression)
  extends MatchLike {

  children = simpleMatches :+ where

  override def checkWithContext(context: Context): Unit =
    simpleMatches.foreach(_.checkWithContext(context))
}

case class SimpleMatchClause(graphPattern: GraphPattern, graph: Graph) extends MatchLike {

  children = List(graphPattern, graph)

  override def checkWithContext(context: Context): Unit = {
    val graphDb = context.asInstanceOf[QueryContext].graphDb
    val graphPatternContext: GraphPatternContext = {
      graph match {
        case DefaultGraph() =>
          if (graphDb.hasDefaultGraph)
            GraphPatternContext(
              schema = graphDb.defaultGraph(), graphName = graphDb.defaultGraph().graphName)
          else
            throw DefaultGraphNotAvailableException()

        case NamedGraph(graphName) =>
          if (graphDb.hasGraph(graphName))
            GraphPatternContext(schema = graphDb.graph(graphName), graphName = graphName)
          else
            throw NamedGraphNotAvailableException(graphName)

        case QueryGraph(_) => // TODO: What checks should we add here?
          throw UnsupportedOperation("Query graphs are not supported.")
      }
    }

    graphPattern.topology.foreach(_.checkWithContext(graphPatternContext))
  }
}
