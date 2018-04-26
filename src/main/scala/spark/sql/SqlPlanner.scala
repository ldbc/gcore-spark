package spark.sql

import algebra.operators._
import algebra.target_api._
import algebra.trees.{AlgebraToTargetTree, AlgebraTreeNode}
import algebra.types.Graph
import algebra.{target_api => target}
import compiler.CompileContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import schema.GraphDb
import spark.sql.SqlPlanner.BINDING_TABLE_GLOBAL_VIEW
import spark.sql.operators._
import spark.sql.{operators => sql}

object SqlPlanner {
  val BINDING_TABLE_GLOBAL_VIEW: String = "BindingTableGlobalView"
}

/** Creates a physical plan with textual SQL queries. */
case class SqlPlanner(compileContext: CompileContext) extends TargetPlanner {

  override type StorageType = DataFrame

  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(compileContext.graphDb, this)
  val sparkSession: SparkSession = compileContext.sparkSession
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def solveBindingTable(matchClause: AlgebraTreeNode): DataFrame = {
    val sparkTree: TargetTreeNode = rewriter.rewriteTree(matchClause).asInstanceOf[TargetTreeNode]
    logger.info("\n{}", sparkTree.treeString())
    val btable: SqlBindingTableMetadata =
      sparkTree.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    btable.solveBtableOps(sparkSession)
  }

  // TODO: This method needs to return a PathPropertyGraph, built from the currently returned
  // sequence of DataFrames.
  override def constructGraph(btable: DataFrame,
                              constructClauses: Seq[AlgebraTreeNode]): Seq[DataFrame] = {
    // Register the resulting binding table as a view, so that each construct clause can reuse it.
    btable.createOrReplaceGlobalTempView(BINDING_TABLE_GLOBAL_VIEW)
    val sparkTrees: Seq[TargetTreeNode] =
      constructClauses.map(clause => rewriter.rewriteTree(clause).asInstanceOf[TargetTreeNode])

    sparkTrees.flatMap {
      case vertexTree: target.VertexCreate =>
        logger.info("\n{}", vertexTree.treeString())
        val btable: SqlBindingTableMetadata =
          vertexTree.bindingTable.asInstanceOf[SqlBindingTableMetadata]
        val entityData: DataFrame = btable.solveBtableOps(sparkSession)
        entityData.show()

        Seq(entityData)

      case edgeTree: target.EdgeCreate =>
        logger.info("\n{}", edgeTree.treeString())
        val btable: SqlBindingTableMetadata =
          edgeTree.bindingTable.asInstanceOf[SqlBindingTableMetadata]
        val entityData: DataFrame = btable.solveBtableOps(sparkSession)
        entityData.show()

        Seq(entityData)
    }
  }

  override def planVertexScan(vertexRelation: VertexRelation, graph: Graph, graphDb: GraphDb)
  : target.VertexScan = sql.VertexScan(vertexRelation, graph, graphDb)

  override def planEdgeScan(edgeRelation: EdgeRelation, graph: Graph, graphDb: GraphDb)
  : target.EdgeScan = sql.EdgeScan(edgeRelation, graph, graphDb)

  override def planPathScan(pathRelation: StoredPathRelation, graph: Graph, graphDb: GraphDb)
  : target.PathScan = sql.PathScan(pathRelation, graph, graphDb)

  override def planUnionAll(unionAllOp: algebra.operators.UnionAll): target.UnionAll =
    sql.UnionAll(
      lhs = unionAllOp.children.head.asInstanceOf[TargetTreeNode],
      rhs = unionAllOp.children.last.asInstanceOf[TargetTreeNode])

  override def planJoin(joinOp: JoinLike): target.Join = {
    val lhs: TargetTreeNode = joinOp.children.head.asInstanceOf[TargetTreeNode]
    val rhs: TargetTreeNode = joinOp.children.last.asInstanceOf[TargetTreeNode]
    joinOp match {
      case _: algebra.operators.InnerJoin => sql.InnerJoin(lhs, rhs)
      case _: algebra.operators.CrossJoin => sql.CrossJoin(lhs, rhs)
      case _: algebra.operators.LeftOuterJoin => sql.LeftOuterJoin(lhs, rhs)
    }
  }

  override def planSelect(selectOp: algebra.operators.Select): target.Select =
    sql.Select(selectOp.children.head.asInstanceOf[TargetTreeNode], selectOp.expr)

  override def createBindingTablePlaceholder: target.MatchBindingTable =
    sql.MatchBindingTable(sparkSession)

  override def planProject(projectOp: algebra.operators.Project): target.Project =
    sql.Project(projectOp.children.head.asInstanceOf[TargetTreeNode], projectOp.attributes.toSeq)

  override def planGroupBy(groupByOp: algebra.operators.GroupBy): target.GroupBy =
    sql.GroupBy(
      relation = groupByOp.getRelation.asInstanceOf[TargetTreeNode],
      groupingAttributes = groupByOp.getGroupingAttributes,
      aggregateFunctions = groupByOp.getAggregateFunction,
      having = groupByOp.getHaving)

  override def planAddColumn(addColumnOp: algebra.operators.AddColumn): target.AddColumn =
    sql.AddColumn(
      reference = addColumnOp.reference,
      relation =
        sql.Project(
          relation = addColumnOp.children.last.asInstanceOf[TargetTreeNode],
          attributes =
            (addColumnOp.relation.getBindingSet.refSet ++ Set(addColumnOp.reference)).toSeq))

  override def planEntityConstruct(entityConstruct: EntityConstructRelation)
  : target.EntityConstruct = {

    sql.EntityConstruct(
      reference = entityConstruct.reference,
      isMatchedRef = entityConstruct.isMatchedRef,
      relation = entityConstruct.children(1).asInstanceOf[TargetTreeNode],
      groupedAttributes = entityConstruct.groupedAttributes,
      expr = entityConstruct.expr,
      setClause = entityConstruct.setClause,
      removeClause = entityConstruct.removeClause)
  }

  override def planVertexCreate(vertexConstruct: VertexConstructRelation): target.VertexCreate =
    sql.VertexCreate(
      reference = vertexConstruct.reference,
      relation = vertexConstruct.children.last.asInstanceOf[TargetTreeNode])

  override def planEdgeCreate(edgeConstruct: EdgeConstructRelation): target.EdgeCreate =
    sql.EdgeCreate(
      reference = edgeConstruct.reference,
      leftReference = edgeConstruct.leftReference,
      rightReference = edgeConstruct.rightReference,
      connType = edgeConstruct.connType,
      relation = edgeConstruct.children(1).asInstanceOf[TargetTreeNode])
}
