package planner.operators

import algebra.types.{DefaultGraph, Graph, NamedGraph}
import planner.target_api.TargetPlanner
import planner.trees.{PlannerContext, TargetTreeNode}
import schema.PathPropertyGraph

abstract class PhysEntityScan(graph: Graph,
                              plannerContext: PlannerContext,
                              targetPlanner: TargetPlanner) extends TargetTreeNode(targetPlanner) {

  val physGraph: PathPropertyGraph = graph match {
    case DefaultGraph() => plannerContext.graphDb.defaultGraph()
    case NamedGraph(graphName) => plannerContext.graphDb.graph(graphName)
  }
}
