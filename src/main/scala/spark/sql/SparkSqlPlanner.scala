package spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.operators._
import planner.target_api.TargetPlanner
import planner.trees.{PlannerContext, PlannerTreeNode}
import spark.sql.operators.{SparkEdgeScan, SparkVertexScan, SqlQuery}

case class SparkSqlPlanner(spark: SparkSession, tree: PlannerTreeNode, context: PlannerContext)
  extends TargetPlanner {

  override type QueryOperand = SqlQuery
  override type StorageType = DataFrame

  override def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan[SqlQuery] =
    SparkVertexScan(
      vertexScan = vertexScanOp,
      graph = vertexScanOp.graph,
      targetPlanner = this,
      plannerContext = vertexScanOp.context)

  override def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan[SqlQuery] =
    SparkEdgeScan(
      edgeScan = edgeScanOp,
      graph = edgeScanOp.graph,
      plannerContext = edgeScanOp.context,
      targetPlanner = this)
}
