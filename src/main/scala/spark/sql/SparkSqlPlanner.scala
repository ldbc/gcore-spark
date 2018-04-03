package spark.sql

import algebra.operators._
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.operators.{EdgeScan, PathScan, VertexScan}
import planner.target_api._
import planner.trees.{PlannerToTargetTree, PlannerTreeNode, TargetTreeNode}
import spark.sql.operators._

case class SparkSqlPlanner(sparkSession: SparkSession) extends TargetPlanner {

  override type StorageType = DataFrame

  override def createBindingTable(input: PlannerTreeNode): DataFrame = {
    val rewriter: PlannerToTargetTree = PlannerToTargetTree(this)
    val sparkTree: TargetTreeNode = rewriter.rewriteTree(input).asInstanceOf[TargetTreeNode]
    val btable: SparkBindingTable = sparkTree.bindingTable.asInstanceOf[SparkBindingTable]
    btable.solveBtableOps(sparkSession)
  }

  override def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan =
    SparkVertexScan(vertexScanOp)

  override def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan = SparkEdgeScan(edgeScanOp)

  override def createPhysPathScan(pathScanOp: PathScan): PhysPathScan = SparkPathScan(pathScanOp)

  override def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll =
    SparkUnionAll(
      lhs = unionAllOp.children.head.asInstanceOf[TargetTreeNode],
      rhs = unionAllOp.children.last.asInstanceOf[TargetTreeNode])

  override def createPhysJoin(joinOp: JoinLike): PhysJoin = {
    val lhs: TargetTreeNode = joinOp.children.head.asInstanceOf[TargetTreeNode]
    val rhs: TargetTreeNode = joinOp.children.last.asInstanceOf[TargetTreeNode]
    joinOp match {
      case _: InnerJoin => SparkInnerJoin(lhs, rhs)
      case _: CrossJoin => SparkCrossJoin(lhs, rhs)
      case _: LeftOuterJoin => SparkLeftOuterJoin(lhs, rhs)
    }
  }

  override def createPhysSelect(selectOp: Select): PhysSelect =
    SparkSelect(selectOp.children.head.asInstanceOf[TargetTreeNode], selectOp.expr)
}
