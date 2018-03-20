package planner.operators

import algebra.types.Graph
import planner.trees.{PlannerContext, PlannerTreeNode}

abstract class EntityScan(graph: Graph, context: PlannerContext) extends PlannerTreeNode
