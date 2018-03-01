package ir.algebra.types

import ir.algebra.operators.Query

/** A graph that is being queried. */
abstract class Graph extends AlgebraType

/** The default graph in this database. */
case class DefaultGraph() extends Graph {
  override def isLeaf: Boolean = true
}

/** A graph specified through its name. */
case class NamedGraph(graphName: String) extends Graph {
  override def isLeaf: Boolean = true

  override def toString: String = s"$name [$graphName]"
}

/** A graph specified through a query. */
case class QueryGraph(query: Query) extends Graph {
  children = List(query)
}
