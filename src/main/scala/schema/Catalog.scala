/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package schema

object Catalog {
  val empty: Catalog = new Catalog { override type StorageType = Nothing }

  val START_BASE_TABLE_INDEX = 1000000 // 1_000_000
  val TABLE_INDEX_INCREMENT = 100000 // 100_000

  private var baseEntityTableIndex: Int = START_BASE_TABLE_INDEX

  // TODO: Should this be synchronized?
  def nextBaseEntityTableIndex: Int = {
    val nextIndex: Int = baseEntityTableIndex
    baseEntityTableIndex += TABLE_INDEX_INCREMENT
    nextIndex
  }

  // TODO: Should this be synchronized?
  def resetBaseEntityTableIndex(): Unit = baseEntityTableIndex = START_BASE_TABLE_INDEX
}

/** Keeps track of all the [[PathPropertyGraph]]s available for querying. */
abstract class Catalog {

  type StorageType

  private var registeredGraphs: Map[String, PathPropertyGraph] = Map.empty
  private var registeredDefaultGraph: PathPropertyGraph = PathPropertyGraph.empty

  var databaseDirectory: String = "Default"

  /**
    * All available [[PathPropertyGraph]]s in this database (the default graph is by default
    * included in the result).
    */
  def allGraphs: Seq[PathPropertyGraph] = registeredGraphs.values.toSeq

  /** Checks whether a graph given by its name has been registered in this database. */
  def hasGraph(graphName: String): Boolean = registeredGraphs.contains(graphName)

  /**
    * Returns the [[PathPropertyGraph]] given by its name if it has been registered in the database,
    * or else the empty [[PathPropertyGraph]].
    */
  def graph(graphName: String): PathPropertyGraph =
    registeredGraphs.getOrElse(graphName, PathPropertyGraph.empty)

  /**
    * Registers a [[PathPropertyGraph]] with this database. This means that the graph can be then
    * queried.
    */
  def registerGraph(graph: PathPropertyGraph): Unit =
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
      registeredDefaultGraph = PathPropertyGraph.empty
  }

  /**
    * @see [[unregisterGraph(graphName: String)]]
    */
  def unregisterGraph(graph: PathPropertyGraph): Unit =
    unregisterGraph(graph.graphName)

  /** Checks whether a default graph has been defined for this database. */
  def hasDefaultGraph: Boolean = registeredDefaultGraph != PathPropertyGraph.empty

  /** Returns the default [[PathPropertyGraph]] in this database. */
  def defaultGraph(): PathPropertyGraph = registeredDefaultGraph

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
  def resetDefaultGraph(): Unit = registeredDefaultGraph = PathPropertyGraph.empty


  override def toString: String = {
    s"Default graph: ${registeredDefaultGraph.graphName}\nAvailable graphs: " +
      s"${registeredGraphs.keys.mkString(",")}"
  }
}
