package spark.sql

import algebra.operators._
import algebra.target_api._
import algebra.trees.{AlgebraToTargetTree, AlgebraTreeNode}
import algebra.types.Graph
import algebra.{target_api => target}
import compiler.CompileContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import schema.Catalog
import spark.sql.operators._
import spark.sql.{operators => sql}

/** Creates a physical plan with textual SQL queries. */
case class SqlPlanner(compileContext: CompileContext) extends TargetPlanner {

  override type StorageType = DataFrame

  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(compileContext.catalog, this)
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
    btable.createOrReplaceGlobalTempView(algebra.trees.BasicToGroupConstruct.BTABLE_VIEW)
    // The root of each tree is a GroupConstruct.

    constructClauses.map(constructClause => {
      val groupConstruct: GroupConstruct = constructClause.asInstanceOf[GroupConstruct]

      // Rewrite the filtered binding table and register it as a global view.
      val baseConstructTable: TargetTreeNode =
        rewriter.rewriteTree(groupConstruct.getBaseConstructTable).asInstanceOf[TargetTreeNode]
      logger.info("\n{}", baseConstructTable.treeString())
      val baseConstructTableData: DataFrame =
        baseConstructTable.bindingTable.asInstanceOf[SqlBindingTableMetadata]
          .solveBtableOps(sparkSession)
      baseConstructTableData.createOrReplaceGlobalTempView(groupConstruct.baseConstructViewName)

      if (baseConstructTableData.rdd.isEmpty()) {
        // It can happen that the GroupConstruct filters on contradictory predicates. Example:
        // CONSTRUCT (c) WHEN c.prop > 3, (c)-... WHEN c.prop <= 3 ...
        // In this case, the resulting baseConstructTable will be the empty DataFrame. We should
        // return here the empty DF as well. No other operations on this table will make sense.
        sparkSession.emptyDataFrame
      } else {
        // Rewrite the vertex table.
        val vertexConstructTable: TargetTreeNode =
          rewriter.rewriteTree(groupConstruct.getVertexConstructTable).asInstanceOf[TargetTreeNode]
        logger.info("\n{}", vertexConstructTable.treeString())
        val vertexData: DataFrame =
          vertexConstructTable.bindingTable.asInstanceOf[SqlBindingTableMetadata]
            .solveBtableOps(sparkSession)

        // For the edge table, if it's not the empty relation, register the vertex table as a global
        // view and solve the query to create the edge table.
        groupConstruct.getEdgeConstructTable match {
          case RelationLike.empty => vertexData
          case relation@_ =>
            vertexData.createOrReplaceGlobalTempView(groupConstruct.vertexConstructViewName)
            val edgeConstructTable: TargetTreeNode =
              rewriter.rewriteTree(relation).asInstanceOf[TargetTreeNode]
            logger.info("\n{}", edgeConstructTable.treeString())
            val vertexAndEdgeData: DataFrame =
              edgeConstructTable.bindingTable.asInstanceOf[SqlBindingTableMetadata]
                .solveBtableOps(sparkSession)
            vertexAndEdgeData
        }
      }
    })
  }

  override def planVertexScan(vertexRelation: VertexRelation, graph: Graph, catalog: Catalog)
  : target.VertexScan = sql.VertexScan(vertexRelation, graph, catalog)

  override def planEdgeScan(edgeRelation: EdgeRelation, graph: Graph, catalog: Catalog)
  : target.EdgeScan = sql.EdgeScan(edgeRelation, graph, catalog)

  override def planPathScan(pathRelation: StoredPathRelation, graph: Graph, catalog: Catalog)
  : target.PathScan = sql.PathScan(pathRelation, graph, catalog)

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

  override def createTableView(viewName: String): target.TableView =
    sql.TableView(viewName, sparkSession)

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

  override def planConstruct(entityConstruct: ConstructRelation)
  : target.EntityConstruct = {

    sql.EntityConstruct(
      reference = entityConstruct.reference,
      isMatchedRef = entityConstruct.isMatchedRef,
      relation = entityConstruct.children(1).asInstanceOf[TargetTreeNode],
      groupedAttributes = entityConstruct.groupedAttributes,
      expr = entityConstruct.expr,
      setClause = entityConstruct.setClause,
      removeClause = entityConstruct.propAggRemoveClause)
  }
}
