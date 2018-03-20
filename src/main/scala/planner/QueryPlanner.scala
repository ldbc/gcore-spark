package planner

import algebra.trees.AlgebraTreeNode
import compiler.PlanningStage
import planner.trees.{AlgebraToPlannerTree, PlannerContext, PlannerTreeNode}

case class QueryPlanner(context: PlannerContext) extends PlanningStage {

  override def plan(tree: AlgebraTreeNode): PlannerTreeNode = {
    val physOpsTree: PlannerTreeNode =
      (AlgebraToPlannerTree(context) rewriteTree tree).asInstanceOf[PlannerTreeNode]
    physOpsTree printTree()
    physOpsTree
  }
}
