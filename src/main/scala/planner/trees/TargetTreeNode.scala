package planner.trees

import algebra.types.{DefaultGraph, Graph, NamedGraph}
import planner.operators.BindingTable
import schema.PathPropertyGraph

abstract class TargetTreeNode(graph: Graph, plannerContext: PlannerContext)
  extends PlannerTreeNode {

  val bindingTable: BindingTable

  val physGraph: PathPropertyGraph = graph match {
    case DefaultGraph() => plannerContext.graphDb.defaultGraph()
    case NamedGraph(graphName) => plannerContext.graphDb.graph(graphName)
  }
}
