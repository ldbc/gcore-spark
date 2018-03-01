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

  def allGraphs: Seq[PathPropertyGraph[T]]

  // TODO: Add registerGraph, hasGraph, etc.
}
