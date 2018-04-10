package algebra.operators

import algebra.types.{Graph, NamedGraph, QueryGraph}
import common.compiler.Context

/**
  * The union of [[NamedGraph]]s and/or [[QueryGraph]]s. The resulting graph will contain the
  * vertices, edges and stored paths of all the graphs participating in the union.
  */
case class GraphUnion(graphs: Seq[Graph]) extends GcoreOperator {

  children = graphs

  override def checkWithContext(context: Context): Unit = {}
}
