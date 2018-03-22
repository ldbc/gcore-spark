package spark.sql

import algebra.operators._
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.operators._
import planner.target_api._
import planner.trees.{PlannerContext, PlannerTreeNode, TargetTreeNode}
import spark.sql.operators._

case class SparkSqlPlanner(spark: SparkSession, tree: PlannerTreeNode, context: PlannerContext)
  extends TargetPlanner {

  override type StorageType = DataFrame

  override def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan =
    SparkVertexScan(
      vertexScan = vertexScanOp,
      graph = vertexScanOp.graph,
      plannerContext = vertexScanOp.context)

  override def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan =
    SparkEdgeScan(
      edgeScan = edgeScanOp,
      graph = edgeScanOp.graph,
      plannerContext = edgeScanOp.context)

  override def createPhysPathScan(pathScanOp: PathScan): PhysPathScan =
    SparkPathScan(
      pathScan = pathScanOp,
      graph = pathScanOp.graph,
      plannerContext = pathScanOp.context)

  override def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll =
    SparkUnionAll(
      lhs = unionAllOp.children.head.asInstanceOf[TargetTreeNode],
      rhs = unionAllOp.children.last.asInstanceOf[TargetTreeNode])

  override def createPhysJoin(joinOp: JoinLike): PhysJoin = joinOp match {
    case InnerJoin(_, _, _) =>
      SparkInnerJoin(
        lhs = joinOp.children.head.asInstanceOf[TargetTreeNode],
        rhs = joinOp.children.last.asInstanceOf[TargetTreeNode])
    case CrossJoin(_, _, _) =>
      SparkCrossJoin(
        lhs = joinOp.children.head.asInstanceOf[TargetTreeNode],
        rhs = joinOp.children.last.asInstanceOf[TargetTreeNode])
    case LeftOuterJoin(_, _, _) =>
      SparkLeftOuterJoin(
        lhs = joinOp.children.head.asInstanceOf[TargetTreeNode],
        rhs = joinOp.children.last.asInstanceOf[TargetTreeNode])
  }
}
