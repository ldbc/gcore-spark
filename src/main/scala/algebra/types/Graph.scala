package algebra.types

import algebra.operators.Query

/** A graph used in G-CORE queries. */
abstract class Graph extends AlgebraType

/** The default graph in this database. */
case object DefaultGraph extends Graph

/** A graph specified through its name. */
case class NamedGraph(graphName: String) extends Graph {
  override def toString: String = s"$name [$graphName]"
}

/** A graph specified through a sub-query. */
// TODO: Should this or a Query be a PathPropertyGraph?
case class QueryGraph(query: Query) extends Graph {
  children = List(query)
}
