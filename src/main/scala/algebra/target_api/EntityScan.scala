package algebra.target_api

import algebra.types.{DefaultGraph, Graph, NamedGraph}
import schema.{Catalog, PathPropertyGraph}

abstract class EntityScan(graph: Graph, catalog: Catalog) extends TargetTreeNode {

  val physGraph: PathPropertyGraph = graph match {
    case DefaultGraph => catalog.defaultGraph()
    case NamedGraph(graphName) => catalog.graph(graphName)
  }
}
