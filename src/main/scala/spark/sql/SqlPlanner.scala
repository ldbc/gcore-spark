/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.sql

import algebra.expressions._
import algebra.operators.Column._
import algebra.operators._
import algebra.target_api._
import algebra.trees.{AlgebraToTargetTree, AlgebraTreeNode}
import algebra.types.Graph
import algebra.{target_api => target}
import common.RandomNameGenerator.randomString
import compiler.CompileContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import schema.EntitySchema.LabelRestrictionMap
import schema._
import spark.SparkGraph
import spark.sql.SqlPlanner.{ConstructClauseData, GRAPH_NAME_LENGTH, GROUPED_VERTEX_PREFIX}
import spark.sql.SqlQuery.expandExpression
import spark.sql.operators._
import spark.sql.{operators => sql}

import scala.collection._

object SqlPlanner {
  val GRAPH_NAME_LENGTH: Int = 8
  val GROUPED_VERTEX_PREFIX: String = "GroupedVertex"

  /**
    * A subset of the graph data produced by a [[GroupConstruct]]. As a CONSTRUCT clause is
    * rewritten into multiple [[GroupConstruct]]s, we first collect the results of each of them,
    * then create a [[SparkGraph]] from their union.
    */
  sealed case class ConstructClauseData(vertexDataMap: Map[Reference, Table[DataFrame]],
                                        edgeDataMap: Map[Reference, Table[DataFrame]],
                                        edgeRestrictions: LabelRestrictionMap)
}

/** Creates a physical plan with textual SQL queries. */
case class SqlPlanner(compileContext: CompileContext) extends TargetPlanner {

  override type StorageType = DataFrame

  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(compileContext.catalog, this)
  val sparkSession: SparkSession = compileContext.sparkSession
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def solveBindingTable(matchClause: AlgebraTreeNode): DataFrame = {
    val matchData: DataFrame = rewriteAndSolveBtableOps(matchClause)
    matchData.show // log the binding table
    matchData.cache()
  }

  override def constructGraph(btable: DataFrame,
                              groupConstructs: Seq[AlgebraTreeNode]): PathPropertyGraph = {
    // Create a view of the binding table, as the table will be used to build each variable in the
    // CONSTRUCT block.
    btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)

    val constructClauseData: Seq[ConstructClauseData] = groupConstructs.map(treeNode => {
      val groupConstruct: GroupConstruct = treeNode.asInstanceOf[GroupConstruct]
      val baseConstructTableDF = rewriteAndSolveBtableOps(groupConstruct.baseConstructTable)
      if (baseConstructTableDF.rdd.isEmpty()) {
        logger.info("The base construct table was empty, cannot build edge table.")
        ConstructClauseData(
          vertexDataMap = Map.empty,
          edgeDataMap = Map.empty,
          edgeRestrictions = SchemaMap.empty)
      } else {
        // Register a view over the filtered binding table, if it was not empty. We can now start
        // building the vertices of this group construct.
        baseConstructTableDF
          .createOrReplaceGlobalTempView(groupConstruct.baseConstructTableView.viewName)

        val vertexData: mutable.ArrayBuffer[(Reference, Table[DataFrame])] =
          new mutable.ArrayBuffer()
        val edgeData: mutable.ArrayBuffer[(Reference, Table[DataFrame])] =
          new mutable.ArrayBuffer()
        val vertexConstructTable = groupConstruct.vertexConstructTable

        // First, create each unmatched ungrouped vertex.
        vertexConstructTable.unmatchedUngroupedRules.foreach(constructRule => {
          val vdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)
          val entity: Table[DataFrame] =
            createEntity(
              constructRule.reference,
              constructRule.setAssignments,
              constructRule.removeAssignments,
              vdata)
          val vertexTuple: (Reference, Table[DataFrame]) = (constructRule.reference, entity)
          vertexData += vertexTuple
          // We need to register the vertex data as a global view, because it will be used by the
          // next vertex in the list.
          vdata.createOrReplaceGlobalTempView(constructRule.constructRelationTableView.get.viewName)
        })

        vertexConstructTable.matchedRules.foreach(constructRule => {
          val vdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)
          val entity: Table[DataFrame] =
            createEntity(
              constructRule.reference,
              constructRule.setAssignments,
              constructRule.removeAssignments,
              vdata)
          val vertexTuple: (Reference, Table[DataFrame]) = (constructRule.reference, entity)
          vertexData += vertexTuple
        })

        val lastVertexTableViewName: String = {
          if (vertexConstructTable.unmatchedUngroupedRules.nonEmpty) {
            // The last table view of the unmatched ungrouped vertices will contain all of them. It
            // also contains all the matched vertices. We need to add the unmatched and grouped
            // vertices with a join on the grouping attributes, so that we can start building the
            // edges.
            val lastVertexTableView: ConstructRelationTableView =
              vertexConstructTable.unmatchedUngroupedRules.last.constructRelationTableView.get
              s"global_temp.${lastVertexTableView.viewName}"
          } else {
            // If we haven't built any unmatched ungrouped vertex, then the base construct table is
            // the one we need to use to build the grouped vertices. It already contains the matched
            // vertices.
            s"global_temp.${groupConstruct.baseConstructTableView.viewName}"
          }
        }
        val vtableQuery: String = vertexConstructTable.unmatchedGroupedRules
          // The accumulator starts with a full projection, because if don't have any unmatched
          // grouped vertices to build, we will simply return the base construct table.
          .foldLeft(s"SELECT * FROM $lastVertexTableViewName") {
            (prevQuery, constructRule) => {
              val currVdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)
              val entity: Table[DataFrame] =
                createEntity(
                  constructRule.reference,
                  constructRule.setAssignments,
                  constructRule.removeAssignments,
                  currVdata)
              val vertexTuple: (Reference, Table[DataFrame]) = (constructRule.reference, entity)
              vertexData += vertexTuple

              // Join back to prevQuery.
              val vdataViewName: String = s"${GROUPED_VERTEX_PREFIX}_${randomString()}"
              currVdata.createOrReplaceGlobalTempView(vdataViewName)
              val groupedAttrs: Seq[String] =
                constructRule.groupDeclaration.get.groupingSets.map {
                  case PropertyRef(ref, propKey) => s"`${ref.refName}$$${propKey.key}`"
                }
              val vertexRef: String =
                s"`${constructRule.reference.refName}$$${ID_COL.columnName}`"
              val projectAttrs: String = (groupedAttrs :+ vertexRef).mkString(", ")
              val joinKey: String = groupedAttrs.mkString(", ")

              val joinQuery: String =
                s"""
                SELECT * FROM (
                (SELECT $projectAttrs FROM global_temp.$vdataViewName)
                INNER JOIN ($prevQuery) USING ($joinKey))"""

              joinQuery
            }
          }

        // We now have all the vertices into one table (or at least the vertex identities, we can
        // start building the edges.
        val vertexTableDF = sparkSession.sql(vtableQuery)
        vertexTableDF
          .createOrReplaceGlobalTempView(groupConstruct.vertexConstructTableView.viewName)
        val edgeFromTo: mutable.ArrayBuffer[(Reference, (Reference, Reference))] =
          new mutable.ArrayBuffer()
        groupConstruct.edgeConstructRules.foreach(constructRule => {
          val edata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)
          val entity: Table[DataFrame] =
            createEntity(
              constructRule.reference,
              constructRule.setAssignments,
              constructRule.removeAssignments,
              edata,
              Some(constructRule.fromRef.get, constructRule.toRef.get))
          val edgeTuple: (Reference, Table[DataFrame]) = (constructRule.reference, entity)
          val edgeFromToTuple: (Reference, (Reference, Reference)) =
            (constructRule.reference, (constructRule.fromRef.get, constructRule.toRef.get))
          edgeData += edgeTuple
          edgeFromTo += edgeFromToTuple
        })

        val vertexDataMap: Map[Reference, Table[DataFrame]] = vertexData.toMap
        val edgeDataMap: Map[Reference, Table[DataFrame]] = edgeData.toMap
        val edgeLabelRestrictions: LabelRestrictionMap =
          SchemaMap(
            edgeFromTo.map {
              case (edgeRef, (fromRef, toRef)) =>
                val edgeLabel: Label = edgeDataMap(edgeRef).name
                val fromLabel: Label = vertexDataMap(fromRef).name
                val toLabel: Label = vertexDataMap(toRef).name
                edgeLabel -> (fromLabel, toLabel)
            }.toMap)

        ConstructClauseData(vertexDataMap, edgeDataMap, edgeLabelRestrictions)
      }
    })

    // Union all data from construct clauses into a single PathPropertyGraph.
    val graph: SparkGraph = new SparkGraph {
      override var graphName: String = randomString(length = GRAPH_NAME_LENGTH)

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap =
        constructClauseData.map(_.edgeRestrictions).reduce(_ union _)

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] =
        constructClauseData.map(_.vertexDataMap).reduce(_ ++ _).values.toSeq

      override def edgeData: Seq[Table[DataFrame]] =
        constructClauseData.map(_.edgeDataMap).reduce(_ ++ _).values.toSeq
    }

    logger.info(s"Constructed new graph:\n$graph")
    graph
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
      relation = addColumnOp.children.last.asInstanceOf[TargetTreeNode])

  override def planPathSearch(pathRelation: VirtualPathRelation, graph: Graph, catalog: Catalog)
  : target.PathSearch =
    sql.PathSearch(pathRelation, graph, catalog, sparkSession)

  /** Translates the relation into a SQL query and runs it on Spark's SQL engine. */
  private def rewriteAndSolveBtableOps(relation: AlgebraTreeNode): DataFrame = {
    val sqlRelation: AlgebraTreeNode = rewriter.rewriteTree(relation)
    solveBtableOps(sqlRelation)
  }

  /** Runs a physical plan on Spark's SQL engine. */
  private def solveBtableOps(relation: AlgebraTreeNode): DataFrame = {
    logger.info("\nSolving\n{}", relation.treeString())
    val btableMetadata: SqlBindingTableMetadata =
      relation.asInstanceOf[TargetTreeNode].bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val data: DataFrame = btableMetadata.solveBtableOps(sparkSession)
    data
  }

  private def createEntity(reference: Reference,
                           setAssignments: Seq[AlgebraExpression],
                           removeAssignments: Seq[AlgebraExpression],
                           constructRelation: DataFrame,
                           fromToRefs: Option[(Reference, Reference)] = None): Table[DataFrame] = {
    val labelColSelect: String = s"${reference.refName}$$${TABLE_LABEL_COL.columnName}"

    val entityFields: Set[String] =
      constructRelation.columns
        .filter(_.startsWith(s"${reference.refName}$$"))
        .map(field => s"`$field`")
        .toSet

    // If this is an edge, we need to project the correct source and destination id fields from the
    // construct table.
    val edgeEndps: Set[String] = {
      if (fromToRefs.isDefined) {
        val fromRef: String = fromToRefs.get._1.refName
        val toRef: String = fromToRefs.get._2.refName
        val ref: String = reference.refName
        Set(s"`$fromRef$$${ID_COL.columnName}` AS `$ref$$${FROM_ID_COL.columnName}`",
          s"`$toRef$$${ID_COL.columnName}` AS `$ref$$${TO_ID_COL.columnName}`")
      } else Set.empty
    }
    val setAttributes: Seq[String] =
      setAssignments.collect {
        case PropertySet(ref, PropAssignment(propKey, expr)) =>
          s"(${expandExpression(expr)}) AS `${ref.refName}$$${propKey.key}`"
        case LabelAssignments(Seq(Label(value), _*)) =>
          // TODO: We should check here that the new entity has at most one label. And we should add
          // a label if there is none.
          s""""$value" AS `$labelColSelect`"""
      }
    val removeAttributes: Set[String] =
      removeAssignments.map {
        case PropertyRemove(PropertyRef(ref, PropertyKey(key))) => s"`${ref.refName}$$$key`"
        // TODO: Here too label remove should be implemented such that we make sure we end up with
        // exactly one label for the entity.
        case _: LabelRemove => s"`$labelColSelect`"
      }
      .toSet

    // For edges that had previous source and destination ids, we remove them.
    val removePrevFromTo: Set[String] =
      if (fromToRefs.isDefined) {
        Set(s"`${reference.refName}$$${FROM_ID_COL.columnName}`",
          s"`${reference.refName}$$${TO_ID_COL.columnName}`")
      } else Set.empty

    val selectionList: Seq[String] =
      (entityFields -- removeAttributes -- removePrevFromTo ++ setAttributes ++ edgeEndps).toSeq
    val entityDf: DataFrame = constructRelation.selectExpr(selectionList: _*)
    val label: Label = Label(entityDf.select(labelColSelect).first.getString(0))

    val newColumnNames: Seq[String] =
      entityDf.columns.map(columnName => columnName.split(s"${reference.refName}\\$$")(1))
    val entityDfColumnsRenamed: DataFrame =
      entityDf.toDF(newColumnNames: _*).drop(s"${TABLE_LABEL_COL.columnName}")

    // Log DF before stripping the variable$ prefix.
    entityDf.show
    // Log the final DF that goes into the graph.
    entityDfColumnsRenamed.show

    Table[DataFrame](name = label, data = entityDfColumnsRenamed)
  }
}
