package spark.sql

import algebra.expressions.ObjectConstructPattern
import algebra.operators._
import algebra.trees.AlgebraTreeNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import planner.operators.{EdgeScan, PathScan, VertexCreate, VertexScan}
import planner.target_api._
import planner.trees.{PlannerToTargetTree, PlannerTreeNode, TargetTreeNode}
import spark.sql.SparkSqlPlanner.BINDING_TABLE_GLOBAL_VIEW
import spark.sql.operators._

object SparkSqlPlanner {
  val BINDING_TABLE_GLOBAL_VIEW: String = "BindingTableGlobalView"
}

/** Creates a physical plan with textual SQL queries. */
case class SparkSqlPlanner(sparkSession: SparkSession) extends TargetPlanner {

  override type StorageType = DataFrame

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val rewriter: PlannerToTargetTree = PlannerToTargetTree(this)

  override def solveBindingTable(matchClause: PlannerTreeNode): DataFrame = {
    val sparkTree: TargetTreeNode =
      rewriter.rewriteTree(matchClause).asInstanceOf[TargetTreeNode]
    val btable: SparkBindingTable = sparkTree.bindingTable.asInstanceOf[SparkBindingTable]
    btable.solveBtableOps(sparkSession)
  }

  // TODO: This method needs to return a PathPropertyGraph, built from the currently returned
  // sequence of DataFrames.
  override def constructGraph(btable: DataFrame,
                              constructClauses: Seq[PlannerTreeNode]): Seq[DataFrame] = {
    // Register the resulting binding table as a view, so that each construct clause can reuse it.
    btable.createOrReplaceGlobalTempView(BINDING_TABLE_GLOBAL_VIEW)
    val sparkTrees: Seq[TargetTreeNode] =
      constructClauses.map(clause => rewriter.rewriteTree(clause).asInstanceOf[TargetTreeNode])

    sparkTrees.map(sparkTree => {
      // The root of each tree is SparVertexCreate or SparkEdgeCreate
      val btable: SparkBindingTable = sparkTree.bindingTable.asInstanceOf[SparkBindingTable]
      val entityData: DataFrame = btable.solveBtableOps(sparkSession)
      entityData.show()

      entityData
    })
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

  override def createPhysBindingTable: PhysBindingTable =
    SparkPhysBindingTable(sparkSession)

  override def createPhysProject(projectOp: Project): PhysProject =
    SparkProject(projectOp.children.head.asInstanceOf[TargetTreeNode], projectOp.attributes.toSeq)

  override def createPhysGroupBy(groupByOp: GroupBy): PhysGroupBy =
    SparkGroupBy(
      relation = groupByOp.getRelation.asInstanceOf[TargetTreeNode],
      groupingAttributes = groupByOp.getGroupingAttributes,
      aggregateFunctions = groupByOp.getAggregateFunction,
      having = groupByOp.getHaving)

  override def createPhysVertexCreate(vertexCreateOp: VertexCreate): PhysVertexCreate = {
    SparkVertexCreate(
      reference = vertexCreateOp.reference,
      relation = vertexCreateOp.children(1).asInstanceOf[TargetTreeNode],
      expr = vertexCreateOp.children(2).asInstanceOf[ObjectConstructPattern],
      setClause = vertexCreateOp.getSetClause,
      removeClause = vertexCreateOp.getRemoveClause)
  }
}
