package planner

import algebra.trees.AlgebraTreeNode
import compiler.PlanningStage
import planner.trees.{AlgebraToPlannerTree, PlannerContext, PlannerTreeNode}
import org.slf4j.{Logger, LoggerFactory}

/** The aggregation of all phases that create the logical plan from the algebraic tree. */
case class QueryPlanner(context: PlannerContext) extends PlanningStage {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def plan(tree: AlgebraTreeNode): PlannerTreeNode = {
    val physOpsTree: PlannerTreeNode =
      (AlgebraToPlannerTree(context) rewriteTree tree).asInstanceOf[PlannerTreeNode]
    logger.info("\n{}", physOpsTree.treeString())
    physOpsTree
  }
}
