package planner.target_api

import algebra.types.{DefaultGraph, Graph, NamedGraph}
import planner.trees.{PlannerContext, TargetTreeNode}
import schema.PathPropertyGraph

abstract class PhysEntityScan(graph: Graph, plannerContext: PlannerContext)
  extends TargetTreeNode {

  val physGraph: PathPropertyGraph = graph match {
    case DefaultGraph => plannerContext.graphDb.defaultGraph()
    case NamedGraph(graphName) => plannerContext.graphDb.graph(graphName)
  }
}
