package spark.sql

import algebra.expressions.ObjectConstructPattern
import algebra.operators._
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.operators._
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

  val rewriter: PlannerToTargetTree = PlannerToTargetTree(this)

  override def solveBindingTable(matchClause: PlannerTreeNode): DataFrame = {
    val sparkTree: TargetTreeNode = rewriter.rewriteTree(matchClause).asInstanceOf[TargetTreeNode]
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

    sparkTrees.flatMap {
      case vertexTree: SparkVertexCreate =>
        val btable: SparkBindingTable = vertexTree.bindingTable.asInstanceOf[SparkBindingTable]
        val entityData: DataFrame = btable.solveBtableOps(sparkSession)
        entityData.show()

        Seq(entityData)

      case edgeTree: SparkEdgeCreate =>
        val btable: SparkBindingTable = edgeTree.bindingTable.asInstanceOf[SparkBindingTable]
        val entityData: DataFrame = btable.solveBtableOps(sparkSession)
        entityData.show()

        Seq(entityData)
    }
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

  override def createPhysAddColumn(addColumnOp: AddColumn): PhysAddColumn =
    SparkAddColumn(
      reference = addColumnOp.reference,
      relation =
        SparkProject(
          relation = addColumnOp.children.last.asInstanceOf[TargetTreeNode],
          attributes =
            (addColumnOp.relation.getBindingSet.refSet ++ Set(addColumnOp.reference)).toSeq))

  override def createPhysEntityConstruct(entityConstructOp: EntityConstruct)
  : PhysEntityConstruct = {

    SparkEntityConstruct(
      reference = entityConstructOp.reference,
      isMatchedRef = entityConstructOp.isMatchedRef,
      relation = entityConstructOp.children(1).asInstanceOf[TargetTreeNode],
      groupedAttributes = entityConstructOp.groupedAttributes,
      expr = entityConstructOp.expr,
      setClause = entityConstructOp.setClause,
      removeClause = entityConstructOp.removeClause)
  }

  override def createPhysVertexCreate(vertexCreateOp: VertexCreate): PhysVertexCreate =
    SparkVertexCreate(
      reference = vertexCreateOp.reference,
      relation = vertexCreateOp.children.last.asInstanceOf[TargetTreeNode])

  override def createPhysEdgeCreate(edgeCreateOp: EdgeCreate): PhysEdgeCreate =
    SparkEdgeCreate(
      reference = edgeCreateOp.reference,
      leftReference = edgeCreateOp.leftReference,
      rightReference = edgeCreateOp.rightReference,
      connType = edgeCreateOp.connType,
      relation = edgeCreateOp.children.last.asInstanceOf[TargetTreeNode])
}
