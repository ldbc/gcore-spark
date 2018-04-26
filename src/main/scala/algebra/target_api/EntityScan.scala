package algebra.target_api

import algebra.types.{DefaultGraph, Graph, NamedGraph}
import schema.{GraphDb, PathPropertyGraph}

abstract class EntityScan(graph: Graph, graphDb: GraphDb) extends TargetTreeNode {

  val physGraph: PathPropertyGraph = graph match {
    case DefaultGraph => graphDb.defaultGraph()
    case NamedGraph(graphName) => graphDb.graph(graphName)
  }
}
