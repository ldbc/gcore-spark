package algebra.operators

import algebra.exceptions._
import algebra.expressions.AlgebraExpression
import algebra.trees.{GraphPatternContext, QueryContext}
import algebra.types._
import common.compiler.Context
import common.exceptions.UnsupportedOperation

/** A match-like operator that participates in the match sub-query of a G-CORE query. */
abstract class MatchLike extends GcoreOperator

/**
  * The top-most match clause of the query. It contains a non-optional [[CondMatchClause]] and zero
  * or more optional [[CondMatchClause]]s.
  */
case class MatchClause(nonOptMatches: CondMatchClause, optMatches: Seq[CondMatchClause])
  extends MatchLike {

  children = nonOptMatches +: optMatches

  override def checkWithContext(context: Context): Unit = {
    nonOptMatches.checkWithContext(context)
    optMatches.foreach(_.checkWithContext(context))
  }
}

/**
  * A sequence of [[SimpleMatchClause]]s and a condition over the binding table produced by solving
  * this sequence.
  */
case class CondMatchClause(simpleMatches: Seq[SimpleMatchClause], where: AlgebraExpression)
  extends MatchLike {

  children = simpleMatches :+ where

  override def checkWithContext(context: Context): Unit =
    simpleMatches.foreach(_.checkWithContext(context))
}

/** A [[GraphPattern]] that should be solved within a particular [[Graph]]. */
case class SimpleMatchClause(graphPattern: GraphPattern, graph: Graph) extends MatchLike {

  children = List(graphPattern, graph)

  /**
    * Validate that the [[Graph]] this pattern is matched on has been registered in the database.
    * Otherwise we cannot infer its schema and cannot further run the query on it.
    *
    * [[QueryGraph]]s are not supported in the current version of the interpreter.
    */
  override def checkWithContext(context: Context): Unit = {
    val catalog = context.asInstanceOf[QueryContext].catalog
    val graphPatternContext: GraphPatternContext = {
      graph match {
        case DefaultGraph =>
          if (catalog.hasDefaultGraph)
            GraphPatternContext(
              schema = catalog.defaultGraph(), graphName = catalog.defaultGraph().graphName)
          else
            throw DefaultGraphNotAvailableException()

        case NamedGraph(graphName) =>
          if (catalog.hasGraph(graphName))
            GraphPatternContext(schema = catalog.graph(graphName), graphName = graphName)
          else
            throw NamedGraphNotAvailableException(graphName)

        case _: QueryGraph => // TODO: What checks should we add here?
          throw UnsupportedOperation("Query graphs are not supported.")
      }
    }

    graphPattern.topology.foreach(_.checkWithContext(graphPatternContext))
  }
}
