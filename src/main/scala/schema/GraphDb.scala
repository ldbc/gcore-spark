package schema

import org.apache.spark.sql.DataFrame

/**
  * Keeps track of all the [[PathPropertyGraph]]s available for querying.
  *
  * @tparam T The type of the collection that backs the actual data. Depending on the system used
  *           as a backend, this type may differ. For example, for a Spark solution, we could
  *           represent a table as a [[DataFrame]].
  */
abstract class GraphDb[T] {

  private var registeredGraphs: Map[String, PathPropertyGraph[T]] = Map.empty
  private var registeredDefaultGraph: PathPropertyGraph[T] = PathPropertyGraph.empty[T]

  /**
    * All available [[PathPropertyGraph]]s in this database (the default graph is by default
    * included in the result).
    */
  def allGraphs: Seq[PathPropertyGraph[T]] = registeredGraphs.values.toSeq

  /** Checks whether a graph given by its name has been registered in this database. */
  def hasGraph(graphName: String): Boolean = registeredGraphs.contains(graphName)

  /**
    * Returns the [[PathPropertyGraph]] given by its name if it has been registered in the database,
    * or else the empty [[PathPropertyGraph]].
    */
  def graph(graphName: String): PathPropertyGraph[T] =
    registeredGraphs.getOrElse(graphName, PathPropertyGraph.empty[T])

  /**
    * Registers a [[PathPropertyGraph]] with this database. This means that the graph can be then
    * queried.
    */
  def registerGraph(graph: PathPropertyGraph[T]): Unit =
    registeredGraphs += (graph.graphName -> graph)

  /**
    * Unregisters a [[PathPropertyGraph]] from this database. This means the graph is no longer
    * available for querying. It can be re-registered at any time.
    *
    * If the de-registered graph was the default graph, then the default graph is set to the empty
    * [[PathPropertyGraph]].
    */
  def unregisterGraph(graphName: String): Unit = {
    registeredGraphs -= graphName

    if (graphName == registeredDefaultGraph.graphName)
      registeredDefaultGraph = PathPropertyGraph.empty[T]
  }

  /**
    * @see [[unregisterGraph(graphName: String)]]
    */
  def unregisterGraph(graph: PathPropertyGraph[T]): Unit =
    unregisterGraph(graph.graphName)

  /** Checks whether a default graph has been defined for this database. */
  def hasDefaultGraph: Boolean = registeredDefaultGraph.nonEmpty

  /** Returns the default [[PathPropertyGraph]] in this database. */
  def defaultGraph(): PathPropertyGraph[T] = registeredDefaultGraph

  /**
    * Sets the default [[PathPropertyGraph]] of this database. The graph needs to have been
    * registered with the database before the operation, or else it will throw an exception.
    */
  def setDefaultGraph(graphName: String): Unit = {
    if (!hasGraph(graphName))
      throw SchemaException(s"The graph $graphName has not yet been registered in this database.")

    registeredDefaultGraph = graph(graphName)
  }

  /**
    * Resets the default graph of this database to the empty [[PathPropertyGraph]]. It does not
    * deregister the graph from the database.
    */
  def resetDefaultGraph(): Unit =
    registeredDefaultGraph = PathPropertyGraph.empty[T]


  override def toString: String = {
    s"Default graph: ${registeredDefaultGraph.graphName}\nAvailable graphs: " +
      s"${registeredGraphs.keys.mkString(",")}"
  }
}
