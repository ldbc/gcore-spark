package spark.sql

import algebra.operators._
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.operators._
import planner.target_api.TargetPlanner
import planner.trees.{PlannerContext, PlannerTreeNode, TargetTreeNode}
import spark.sql.operators._

case class SparkSqlPlanner(spark: SparkSession, tree: PlannerTreeNode, context: PlannerContext)
  extends TargetPlanner {

  override type StorageType = DataFrame

  override def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan =
    SparkVertexScan(
      vertexScan = vertexScanOp,
      graph = vertexScanOp.graph,
      targetPlanner = this,
      plannerContext = vertexScanOp.context)

  override def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan =
    SparkEdgeScan(
      edgeScan = edgeScanOp,
      graph = edgeScanOp.graph,
      plannerContext = edgeScanOp.context,
      targetPlanner = this)

  override def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll =
    SparkUnionAll(
      lhs = unionAllOp.children.head.asInstanceOf[TargetTreeNode],
      rhs = unionAllOp.children.last.asInstanceOf[TargetTreeNode],
      targetPlanner = this)

  override def createPhysJoin(joinOp: JoinLike): PhysJoin = joinOp match {
    case InnerJoin(_, _, _) =>
      SparkInnerJoin(
        lhs = joinOp.children.head.asInstanceOf[TargetTreeNode],
        rhs = joinOp.children.last.asInstanceOf[TargetTreeNode],
        targetPlanner = this)
    case CrossJoin(_, _, _) =>
      SparkCrossJoin(
        lhs = joinOp.children.head.asInstanceOf[TargetTreeNode],
        rhs = joinOp.children.last.asInstanceOf[TargetTreeNode],
        targetPlanner = this)
    case LeftOuterJoin(_, _, _) =>
      SparkLeftOuterJoin(
        lhs = joinOp.children.head.asInstanceOf[TargetTreeNode],
        rhs = joinOp.children.last.asInstanceOf[TargetTreeNode],
        targetPlanner = this)
  }
}
