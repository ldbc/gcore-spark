/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import schema.EntitySchema.LabelRestrictionMap
import schema._
import spark.SparkGraph
import spark.sql.SqlPlanner.{ConstructClauseData, GRAPH_NAME_LENGTH, GROUPED_VERTEX_PREFIX}
import spark.sql.SqlQuery.expandExpression
import spark.sql.operators._
import spark.sql.{operators => sql}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object SqlPlanner {
  val GRAPH_NAME_LENGTH: Int = 8
  val GROUPED_VERTEX_PREFIX: String = "GroupedVertex"
  val SOLUTION_IDENTIFICATOR: String = "sid"

  /**
    * A subset of the graph data produced by a [[GroupConstruct]]. As a CONSTRUCT clause is
    * rewritten into multiple [[GroupConstruct]]s, we first collect the results of each of them,
    * then create a [[SparkGraph]] from their union.
    */
  sealed case class ConstructClauseData(vertexDataMap: Map[Reference, Seq[Table[DataFrame]]],
                                        edgeDataMap: Map[Reference, Seq[Table[DataFrame]]],
                                        edgeRestrictions: LabelRestrictionMap,
                                        pathDataMap: Map[Reference, Seq[Table[DataFrame]]],
                                        pathRestrictions: LabelRestrictionMap)
}

/** Creates a physical plan with textual SQL queries. */
case class SqlPlanner(compileContext: CompileContext) extends TargetPlanner {
  val SOLUTION_IDENTIFICATOR: String = "sid"
  val UNLABELED: String ="Unlabeled"
  override type StorageType = DataFrame

  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(compileContext.catalog, this)
  val sparkSession: SparkSession = compileContext.sparkSession
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def solveBindingTable(matchClause: AlgebraTreeNode, matchWhere: AlgebraTreeNode): DataFrame = {
    var matchData: DataFrame = addColumnIndex(SOLUTION_IDENTIFICATOR,rewriteAndSolveBtableOps(matchClause))
    val exp: String = expandExpression(matchWhere)
    matchData = matchData.where(exp)
    matchData.show // log the binding table
    matchData.cache()
  }

  def generateConstructBindingTable(bt: DataFrame,
                                    groupConstructs: Seq[AlgebraTreeNode]):DataFrame={
    var fullCBTable= sparkSession.emptyDataFrame
    var bindtable = bt
    //val constructsBindingTables: mutable.ArrayBuffer[DataFrame] = new mutable.ArrayBuffer()
    var lastUnmatchedUngropuedViewName =""
    groupConstructs.foreach(treeNode =>{
      val constructExp: ConstructExp = treeNode.asInstanceOf[ConstructExp]
      val constructWhereClause = constructExp.where
      val constructHavingClause = constructExp.having
      var cBTable= sparkSession.emptyDataFrame
      var btable = sparkSession.emptyDataFrame
      if (!constructWhereClause.children.isEmpty) {
        val sqlRelation: AlgebraTreeNode = rewriter.rewriteTree(constructWhereClause.children.head)
        val exp: String = expandExpression(sqlRelation)
        btable = bindtable.where(exp)
      }
      else
        btable = bindtable

      btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)

      val vertexTables: mutable.ArrayBuffer[DataFrame] = new mutable.ArrayBuffer()
      val edgeTables: mutable.ArrayBuffer[DataFrame] = new mutable.ArrayBuffer()
      val pathTables: mutable.ArrayBuffer[DataFrame] = new mutable.ArrayBuffer()
      constructExp.children.foreach(treeNode =>{
        val groupConstruct: GroupConstruct = treeNode.asInstanceOf[GroupConstruct]
        val baseConstructTableDF = rewriteAndSolveBtableOps(groupConstruct.baseConstructTable)
        if(!baseConstructTableDF.rdd.isEmpty()) {
          baseConstructTableDF
            .createOrReplaceGlobalTempView(groupConstruct.baseConstructTableView.viewName)

          val vertexConstructTable = groupConstruct.vertexConstructTable
          vertexConstructTable.matchedRules.foreach(constructRule => {
            //val vdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)
            val selectionList = createSelectionList(constructRule.reference,
              constructRule.setAssignments,
              constructRule.removeAssignments,
              btable)
            val vertexTable = btable.selectExpr(selectionList: _*)
            vertexTable.createOrReplaceGlobalTempView(constructRule.constructRelationTableView.get.viewName)
            vertexTables += vertexTable
          })


          vertexConstructTable.unmatchedUngroupedRules.foreach(constructRule => {
            val column_id = constructRule.reference.refName+"$id"
            val column_label = constructRule.reference.refName+"$table_label"
            if(!btable.columns.contains(column_id)){
              val vdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)
              val selectionList = createSelectionList(constructRule.reference,
                constructRule.setAssignments,
                constructRule.removeAssignments,
                vdata)
              val vertexTable = vdata.selectExpr(selectionList: _*)
              val columns = vertexTable.columns.distinct
              vdata.createOrReplaceGlobalTempView(constructRule.constructRelationTableView.get.viewName)
              val selectVertexTable =vertexTable.select(columns.head, columns.tail: _*)
              vertexTables += selectVertexTable
              btable = btable.join(selectVertexTable,Seq(SOLUTION_IDENTIFICATOR),"full")
            }
            else if(btable.where(btable(column_id).isNull).count()>0)
            {
              val tempBtableNulls = btable.where(btable(column_id).isNull).drop(column_id,column_label)
              val tempBtableNotNulls = btable.where(btable(column_id).isNotNull)
              tempBtableNulls.createOrReplaceGlobalTempView(groupConstruct.baseConstructTableView.viewName)
              var vdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)

              val selectionList = createSelectionList(constructRule.reference,
                constructRule.setAssignments,
                constructRule.removeAssignments,
                vdata)
              val vertexTable = vdata.selectExpr(selectionList: _*)
              val columns = vertexTable.columns.distinct
              vdata = vdata.join(tempBtableNotNulls,vdata.columns.intersect(tempBtableNotNulls.columns),"full")
              vdata.createOrReplaceGlobalTempView(constructRule.constructRelationTableView.get.viewName)
              var selectVertexTable =vertexTable.select(columns.head, columns.tail: _*)
              val commCols = selectVertexTable.columns.intersect(tempBtableNotNulls.columns)
              var tempBtableNotNullsFiltered = tempBtableNotNulls.select(commCols.head,commCols.tail:_*)
              selectVertexTable= selectVertexTable
                .join(tempBtableNotNullsFiltered,selectVertexTable.columns.intersect(tempBtableNotNullsFiltered.columns),"full")
              vertexTables += selectVertexTable
              val removeCols= (btable.columns.toSet --  commCols.filterNot(c=> c == SOLUTION_IDENTIFICATOR)).toSeq
              btable = btable.select(removeCols.head,removeCols.tail:_*).join(selectVertexTable,Seq(SOLUTION_IDENTIFICATOR),"full")
            }
            else
              btable.createOrReplaceGlobalTempView(constructRule.constructRelationTableView.get.viewName)

            lastUnmatchedUngropuedViewName= constructRule.constructRelationTableView.get.viewName
            btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)


          })

          var lastVertexTableViewName: String = {
            if (vertexConstructTable.unmatchedUngroupedRules.nonEmpty) {
              // The last table view of the unmatched ungrouped vertices will contain all of them. It
              // also contains all the matched vertices. We need to add the unmatched and grouped
              // vertices with a join on the grouping attributes, so that we can start building the
              // edges.
              /*val lastVertexTableView: ConstructRelationTableView =
              vertexConstructTable.unmatchedUngroupedRules.last.constructRelationTableView.get*/
              s"global_temp.${lastUnmatchedUngropuedViewName}"
            } else {
              // If we haven't built any unmatched ungrouped vertex, then the base construct table is
              // the one we need to use to build the grouped vertices. It already contains the matched
              // vertices.
              lastUnmatchedUngropuedViewName= groupConstruct.baseConstructTableView.viewName
              s"global_temp.${groupConstruct.baseConstructTableView.viewName}"
            }
          }
          var selectAttrGroup: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer()
          val vtableQuery: String = vertexConstructTable.unmatchedGroupedRules
            // The accumulator starts with a full projection, because if don't have any unmatched
            // grouped vertices to build, we will simply return the base construct table.
            .foldLeft(s"SELECT * FROM $lastVertexTableViewName") {
            (prevQuery, constructRule) => {
              if(!btable.columns.contains(constructRule.reference.refName+"$id")) {
                var currVdata: DataFrame = rewriteAndSolveBtableOps(constructRule.constructRelation)

                val selectionList = createSelectionList(constructRule.reference,
                  constructRule.setAssignments,
                  constructRule.removeAssignments,
                  currVdata).filterNot(_ == SOLUTION_IDENTIFICATOR)
                currVdata.createOrReplaceGlobalTempView(constructRule.constructRelationTableView.get.viewName)
                val selectionListAndColumns: Seq[String] = (selectionList ++ currVdata.columns
                  .filterNot(_ == SOLUTION_IDENTIFICATOR).map(col => "`" + col + "`"))

                // Join back to prevQuery.
                val vdataViewName: String = s"${GROUPED_VERTEX_PREFIX}_${randomString()}"
                currVdata.createOrReplaceGlobalTempView(vdataViewName)

                val groupedAttrs: Seq[String] =
                  constructRule.groupDeclaration.get.groupingSets.map {
                    case PropertyRef(ref, propKey) => s"`${ref.refName}$$${propKey.key}`"
                  }
                val vertexRef: String =
                  s"`${constructRule.reference.refName}$$${ID_COL.columnName}`"
                val projectAttrs: String = (groupedAttrs ++ selectionList).mkString(", ")
                selectAttrGroup = selectAttrGroup ++ (groupedAttrs ++ selectionList)
                val joinKey: String = groupedAttrs.mkString(", ")
                val joinQuery: String =
                  s"""
                SELECT * FROM (
                (SELECT $projectAttrs  FROM global_temp.$vdataViewName)
                INNER JOIN ($prevQuery) USING ($joinKey)) """

                joinQuery
              }
              else
                prevQuery
            }
          }

          val vertexTableDF = sparkSession.sql(vtableQuery).selectExpr(selectAttrGroup.distinct:+SOLUTION_IDENTIFICATOR:_*)
          if (!groupConstruct.vertexConstructTable.unmatchedGroupedRules.isEmpty)
          {
            val selectVertexTable = vertexTableDF.toDF(vertexTableDF.columns: _*)
            vertexTables += selectVertexTable
            btable = mergeDF(btable,selectVertexTable,SOLUTION_IDENTIFICATOR)//btable.join(selectVertexTable,selectVertexTable.columns.intersect(btable.columns),"full")
            btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)
          }

          sparkSession.sql(vtableQuery).createOrReplaceGlobalTempView(groupConstruct.vertexConstructTableView.viewName)

          groupConstruct.edgeConstructRules.foreach(constructRule => {
            val edata: DataFrame = if (btable.columns.contains(constructRule.reference.refName+"$id")) btable else rewriteAndSolveBtableOps(constructRule.constructRelation)

            val selectionList= createSelectionList( constructRule.reference,
              constructRule.setAssignments,
              constructRule.removeAssignments,
              edata,
              Some(constructRule.fromRef.get, constructRule.toRef.get))
            val edgeTable= edata.selectExpr(selectionList:_* )
            edgeTables += edgeTable

            btable = mergeDF(btable,edgeTable,SOLUTION_IDENTIFICATOR)//btable.join(edgeTable,Seq(SOLUTION_IDENTIFICATOR),"full")
            btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)
          })


          /**
            *TODO Aldana: path data should always be present in btable right??
            * and if it doesnt i cannot resolve it with solveBtableOps...
            * so i should add an exception if i cant find the path data
            *
            */
          groupConstruct.pathConstructRules.foreach(constructRule => {
            var selectionList = Seq(s"`${constructRule.reference.refName}$$${TO_ID_COL.columnName}`",
              s"`${constructRule.reference.refName}$$${FROM_ID_COL.columnName}`",
              s"`${constructRule.reference.refName}$$${EDGE_SEQ_COL.columnName}`",
              s"`${constructRule.reference.refName}$$${COST_COL.columnName}`",
              s"`${constructRule.reference.refName}$$${ID_COL.columnName}`",
              SOLUTION_IDENTIFICATOR)
            constructRule.setAssignments.foreach(setAssignment => {
              setAssignment match {
                case LabelAssignments(Seq(labels)) =>
                  selectionList = selectionList :+ s""""${labels.value}" AS `${constructRule.reference.refName}$$${TABLE_LABEL_COL.columnName}`"""
              }
            })
            val pathTable = btable.selectExpr(selectionList:_*)
            pathTables += pathTable
            btable = mergeDF(btable, pathTable, SOLUTION_IDENTIFICATOR)
            btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)

          })
        }
      })


      vertexTables.foreach(vt=> {
        cBTable = solutionJoin (cBTable,vt)
        //cBTable.show()
      })
      edgeTables.foreach(et=> {
        val joinId = et.columns.head.split('$').head+"$id"
        cBTable = cBTable.join(et,Seq(SOLUTION_IDENTIFICATOR))
      })
      pathTables.foreach(pt=> {
        cBTable = cBTable.join(pt, Seq(SOLUTION_IDENTIFICATOR))
      })

      if (!constructHavingClause.children.isEmpty) {
        val sqlRelation: AlgebraTreeNode = rewriter.rewriteTree(constructHavingClause.children.head)
        val exp: String = expandExpression(sqlRelation)
        cBTable = cBTable.where(exp)
        btable = btable.where(exp)
      }

       /*println("cbTable")
       cBTable.show()
       println("fullCBTable")
       fullCBTable.show()*/
      fullCBTable = mergeDF(fullCBTable,cBTable,SOLUTION_IDENTIFICATOR)
      //println("merged fullCBTable")
      //fullCBTable.show()
      /* println("btable")
       btable.show()
       println("bindtable")
       bindtable.show()*/
      if(btable.columns.size >= bindtable.columns.size && btable.count() >= bindtable.count())
        bindtable= btable
      else
      {
        //val sids= (btable.select(SOLUTION_IDENTIFICATOR).rdd.flatMap(row => Seq(row(0).asInstanceOf[Long])).collect()).toSeq
        //bindtable = dataframeUnion(btable,bindtable.filter(not(col(SOLUTION_IDENTIFICATOR).isin(sids: _*))))
        bindtable = mergeDF(bindtable,btable,SOLUTION_IDENTIFICATOR)
      }



    })
    println("FULL AGG BINDING TABLE")
    bindtable.show()
    println("FULL CONSTRUCT BINDING TABLE")
    fullCBTable.show()
    fullCBTable
  }

  private def mergeDF(l: DataFrame, r: DataFrame, identificator : String): DataFrame =
  {
    var left:DataFrame = l.alias("left")
    var right:DataFrame = r.alias("right")

    if(left.rdd.isEmpty())
      left = right
    else if (!right.rdd.isEmpty())
    {
      val leftCols = left.columns
      val rightCols = right.columns

      val commonCols = leftCols.toSet intersect rightCols.toSet
      if(commonCols.isEmpty)
        left = left.limit(0).join(right.limit(0))
      else if (commonCols.size== 1 && commonCols.head.equals(identificator))
        left= left.join(right,Seq(identificator),"full")
      else
        left = left
          .join(right,right("right."+identificator)===left("left."+identificator),"full").
          select(leftCols.collect { case c if commonCols.contains(c) =>  (when(left("left."+c).isNull,right("right."+c)).otherwise(left("left."+c))).alias(c)} ++
            leftCols.collect { case c if !commonCols.contains(c) => left("left."+c).alias(c) } ++
            rightCols.collect { case c if !commonCols.contains(c) => right("right."+c).alias(c) } : _*)

    }

    left
  }



  private def solutionJoin (l: DataFrame,r: DataFrame): DataFrame =
  {
    var left:DataFrame = l
    var right:DataFrame = r
    if(left.rdd.isEmpty())
      left = right
    else
    {
      val leftCols = left.columns
      val rightCols = right.columns

      val commonCols = leftCols.toSet intersect rightCols.toSet
      if(commonCols.isEmpty)
        left = left.limit(0).join(right.limit(0))
      else if(commonCols.size==1 && commonCols.head.equals(SOLUTION_IDENTIFICATOR))
      {
        left = left
          .join(right,Seq(SOLUTION_IDENTIFICATOR),"full")
      }
      else
        left = left
          .join(right,right(SOLUTION_IDENTIFICATOR)===left(SOLUTION_IDENTIFICATOR)).
          select(leftCols.collect { case c if commonCols.contains(c) => left(c) } ++
            leftCols.collect { case c if !commonCols.contains(c) => left(c) } ++
            rightCols.collect { case c if !commonCols.contains(c) => right(c) } : _*)
    }

    left
  }


  private def createSelectionList(reference: Reference,
                                  setAssignments: Seq[AlgebraExpression],
                                  removeAssignments: Seq[AlgebraExpression],
                                  constructRelation: DataFrame,
                                  fromToRefs: Option[(Reference, Reference)] = None): Seq[String] = {
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
        if(ref == fromRef && ref == toRef) {
          Set(s"`$ref$$${FROM_ID_COL.columnName}`",
            s"`$ref$$${TO_ID_COL.columnName}`")
        } else {
          Set(s"`$fromRef$$${ID_COL.columnName}` AS `$ref$$${FROM_ID_COL.columnName}`",
            s"`$toRef$$${ID_COL.columnName}` AS `$ref$$${TO_ID_COL.columnName}`")
        }

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

    (entityFields -- removeAttributes -- removePrevFromTo ++ setAttributes ++ edgeEndps).toSeq :+ SOLUTION_IDENTIFICATOR
  }

  override def constructGraph(btable: DataFrame,
                              groupConstructs: Seq[AlgebraTreeNode]): PathPropertyGraph = {
    // Create a view of the binding table, as the table will be used to build each variable in the
    // CONSTRUCT block.
    btable.createOrReplaceGlobalTempView(algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW)

    val constrExp = groupConstructs.asInstanceOf[Seq[ConstructExp]].flatten(conExp =>{conExp.children})

    val constructClauseData: Seq[ConstructClauseData] = constrExp.map(treeNode => {
      val groupConstruct: GroupConstruct = treeNode.asInstanceOf[GroupConstruct]
      val baseConstructTableDF = rewriteAndSolveBtableOps(groupConstruct.baseConstructTable)

      if (baseConstructTableDF.rdd.isEmpty()) {
        logger.info("The base construct table was empty, cannot build edge table.")
        ConstructClauseData(
          vertexDataMap = Map.empty,
          edgeDataMap = Map.empty,
          edgeRestrictions = SchemaMap.empty,
          pathDataMap = Map.empty,
          pathRestrictions = SchemaMap.empty)
      } else {
        // Register a view over the filtered binding table, if it was not empty. We can now start
        // building the vertices of this group construct.
        baseConstructTableDF
          .createOrReplaceGlobalTempView(groupConstruct.baseConstructTableView.viewName)

        val vertexData: mutable.ArrayBuffer[(Reference,  Seq[Table[DataFrame]])] =
          new mutable.ArrayBuffer()
        val edgeData: mutable.ArrayBuffer[(Reference, Seq[Table[DataFrame]])] =
          new mutable.ArrayBuffer()
        val pathData: mutable.ArrayBuffer[(Reference, Seq[Table[DataFrame]])] =
          new mutable.ArrayBuffer()
        val vertexConstructTable = groupConstruct.vertexConstructTable

        // First, create each unmatched ungrouped vertex.
        vertexConstructTable.unmatchedUngroupedRules.foreach(constructRule => {
          val entity:  Seq[Table[DataFrame]] =
            createEntity(
              constructRule.reference,
              btable)
          val vertexTuple: (Reference,  Seq[Table[DataFrame]]) = (constructRule.reference, entity)
          vertexData += vertexTuple
        })

        vertexConstructTable.matchedRules.foreach(constructRule => {

          val entity: Seq[Table[DataFrame]] =
            createEntity(
              constructRule.reference,
              btable)
          val vertexTuple: (Reference,  Seq[Table[DataFrame]]) = (constructRule.reference, entity)
          vertexData += vertexTuple
        })


        vertexConstructTable.unmatchedGroupedRules
          // The accumulator starts with a full projection, because if don't have any unmatched
          // grouped vertices to build, we will simply return the base construct table.
          .foreach(constructRule =>  {
          val entity: Seq[Table[DataFrame]] =
            createEntity(
              constructRule.reference,
              btable)
          val vertexTuple: (Reference,  Seq[Table[DataFrame]]) = (constructRule.reference, entity)
          vertexData += vertexTuple
        }
        )


        // We now have all the vertices into one table (or at least the vertex identities, we can
        // start building the edges.

        val edgeFromTo: mutable.ArrayBuffer[(Reference, (Reference, Reference))] =
          new mutable.ArrayBuffer()
        groupConstruct.edgeConstructRules.foreach(constructRule => {
          val entity: Seq[Table[DataFrame]] =
            createEntity(
              constructRule.reference,
              btable,
              Some(constructRule.fromRef.get, constructRule.toRef.get))
          val edgeTuple: (Reference,  Seq[Table[DataFrame]]) = (constructRule.reference, entity)
          val edgeFromToTuple: (Reference, (Reference, Reference)) =
            (constructRule.reference, (constructRule.fromRef.get, constructRule.toRef.get))
          edgeData += edgeTuple
          edgeFromTo += edgeFromToTuple
        })

        val pathFromTo: mutable.ArrayBuffer[(Reference, (Reference, Reference))] =
          new mutable.ArrayBuffer()
        groupConstruct.pathConstructRules.foreach(constructRule => {
          val entity: Seq[Table[DataFrame]] =
            createEntity(
              constructRule.reference,
              btable,
              Some(constructRule.fromRef.get, constructRule.toRef.get))
          val pathTuple: (Reference, Seq[Table[DataFrame]]) = (constructRule.reference, entity)
          val pathFromToTuple: (Reference, (Reference, Reference)) =
            (constructRule.reference, (constructRule.fromRef.get, constructRule.toRef.get))
          pathData += pathTuple
          pathFromTo += pathFromToTuple
        })

        val vertexDataMap: Map[Reference,  Seq[Table[DataFrame]]] = vertexData.toMap
        val edgeDataMap: Map[Reference,  Seq[Table[DataFrame]]] = edgeData.toMap
        val pathDataMap: Map[Reference, Seq[Table[DataFrame]]] = pathData.toMap
        val edgeLabelRestrictions: LabelRestrictionMap =
          SchemaMap(
            edgeFromTo.map {
              case (edgeRef, (fromRef, toRef)) =>
                edgeRestriction(edgeRef,fromRef,toRef,btable)
                /*val edgeLabel: Label = edgeDataMap(edgeRef).head.name
                val fromLabel: Label = vertexDataMap(fromRef).head.name
                val toLabel: Label = vertexDataMap(toRef).head.name
                edgeLabel -> (fromLabel, toLabel)*/
            }.flatten.toMap)
        val pathLabelRestrictions: LabelRestrictionMap =
          SchemaMap(
            pathFromTo.map {
              case (pathRef, (fromRef, toRef)) =>
                pathRestriction(pathRef,fromRef,toRef,btable)
            }.flatten.toMap
          )

        ConstructClauseData(vertexDataMap, edgeDataMap, edgeLabelRestrictions, pathDataMap, pathLabelRestrictions)
      }
    })

    // Union all data from construct clauses into a single PathPropertyGraph.
    val graph: SparkGraph = new SparkGraph {
      override var graphName: String = randomString(length = GRAPH_NAME_LENGTH)

      override def storedPathRestrictions: LabelRestrictionMap =
        constructClauseData.map(_.pathRestrictions).reduce(_ union _)

      override def edgeRestrictions: LabelRestrictionMap =
        constructClauseData.map(_.edgeRestrictions).reduce(_ union _)

      override def pathData: Seq[Table[DataFrame]] =
        joinDataMap(constructClauseData.map(_.pathDataMap).reduce(_ ++ _).values.flatten.toList)

      override def vertexData: Seq[Table[DataFrame]] =
        joinDataMap(constructClauseData.map(_.vertexDataMap).reduce(_ ++ _).values.flatten.toList)


      override def edgeData: Seq[Table[DataFrame]] =
        joinDataMap(constructClauseData.map(_.edgeDataMap).reduce(_ ++ _).values.flatten.toList)
    }
    logger.info(s"Constructed new graph:\n$graph")

    graph

  }

  // Probar nulls, repetidos.
  private def edgeRestriction (edgeRef: Reference,fromRef:Reference, toRef:Reference, bTable : DataFrame):Seq[(Label,(Label,Label))]=
  {
    val restrictions: mutable.ArrayBuffer[(Label,(Label,Label))] = new mutable.ArrayBuffer()
    val edgeLabelColSelect: String = s"${edgeRef.refName}$$${TABLE_LABEL_COL.columnName}"
    val fromLabelColSelect: String = s"${fromRef.refName}$$${TABLE_LABEL_COL.columnName}"
    val toLabelColSelect: String = s"${toRef.refName}$$${TABLE_LABEL_COL.columnName}"
    val edges = bTable.select(edgeLabelColSelect,fromLabelColSelect,toLabelColSelect).distinct()
edges.show()
    edges.collect().foreach(row =>{
      val edgeLabel: Label = new Label ((if (row.get(0) == null) UNLABELED else row.get(0).toString))
      val fromLabel: Label = new Label ((if (row.get(1) == null) UNLABELED else row.get(1).toString))
      val toLabel: Label = new Label ((if (row.get(2) == null) UNLABELED else row.get(2).toString))
      val rest = edgeLabel -> (fromLabel, toLabel)
      restrictions += rest
    })
    restrictions
  }

  private def pathRestriction (pathRef: Reference,fromRef:Reference, toRef:Reference, bTable : DataFrame):Seq[(Label,(Label,Label))]=
  {
    val restrictions: mutable.ArrayBuffer[(Label,(Label,Label))] = new mutable.ArrayBuffer()
    val pathLabelColSelect: String = s"${pathRef.refName}$$${TABLE_LABEL_COL.columnName}"
    val fromLabelColSelect: String = s"${fromRef.refName}$$${TABLE_LABEL_COL.columnName}"
    val toLabelColSelect: String = s"${toRef.refName}$$${TABLE_LABEL_COL.columnName}"
    val paths = bTable.select(pathLabelColSelect,fromLabelColSelect,toLabelColSelect).distinct()
    paths.show()
    paths.collect().foreach(row =>{
      val pathLabel: Label = new Label ((if (row.get(0) == null) UNLABELED else row.get(0).toString))
      val fromLabel: Label = new Label ((if (row.get(1) == null) UNLABELED else row.get(1).toString))
      val toLabel: Label = new Label ((if (row.get(2) == null) UNLABELED else row.get(2).toString))
      val rest = pathLabel -> (fromLabel, toLabel)
      restrictions += rest
    })
    restrictions
  }

  private def joinDataMap(dataMap :Seq[Table[DataFrame]]): Seq[Table[DataFrame]] =
  {
    var tables= new mutable.HashMap[String,Table[DataFrame]]()
    dataMap.foreach(table => {
      val key = table.name.value
      if(tables.contains(key))
        tables.update(key,new Table[DataFrame](tables.get(key).head.name,mergeDF(tables.get(key).head.data, table.data,"id")))
      else
        tables.put(key,table)
    })

    tables.values.toSeq
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
                           constructRelation: DataFrame,
                           fromToRefs: Option[(Reference, Reference)] = None): Seq[Table[DataFrame]] = {
    val labelColSelect: String = s"${reference.refName}$$${TABLE_LABEL_COL.columnName}"

    val entityFields: Set[String] =
      constructRelation.columns
        .filter(_.startsWith(s"${reference.refName}$$"))
        .map(field => s"`$field`")
        .toSet



    val selectionList: Seq[String] = (entityFields).toSeq
    val entityDf: DataFrame = constructRelation.selectExpr(selectionList: _*).distinct().na.drop("all")
    //val label: Label = Label(entityDf.select(labelColSelect).na.drop("all").first.getString(0))

    val labels = entityDf.select(labelColSelect).distinct().collect().map(_(0)).toList
    val newColumnNames: Seq[String] =
      entityDf.columns.map(columnName => columnName.split(s"${reference.refName.replace("$",s"\\$$")}\\$$")(1))
    // Log DF before stripping the variable$ prefix.
    entityDf.show

    val tables = labels.map(lbl => {

      val entityDfColumnsRenamed: DataFrame =
        entityDf.filter(entityDf(labelColSelect) <=> lbl).toDF(newColumnNames: _*) .drop(s"${TABLE_LABEL_COL.columnName}")

      // Log the final DF that goes into the graph.
      entityDfColumnsRenamed.show
      Table[DataFrame](name = Label( if (lbl == null) UNLABELED else lbl.toString), data = entityDfColumnsRenamed)
    })

    tables
  }



  def addColumnIndex(identificator:String, df: DataFrame):DataFrame ={
    //val id = df.columns.head.split('$').head+"$id"
    //def w = Window.partitionBy().orderBy(id)
    df.withColumn(identificator, monotonically_increasing_id)
  }


}
