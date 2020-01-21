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

package spark.sql.operators

import algebra.expressions.{DisjunctLabels, Reference}
import algebra.operators.Column.{FROM_ID_COL, ID_COL, TABLE_LABEL_COL, TO_ID_COL}
import algebra.operators.VirtualPathRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode, lit, monotonically_increasing_id, size, udf}
import org.slf4j.{Logger, LoggerFactory}
import schema.{Catalog, Table}
import spark.graphx.Utils.createPathData
import spark.sql.SqlQuery
import spark.sql.SqlQuery.{mergeSchemas, refactorScanSchema, selectAllPrependRef}

import scala.collection.mutable

/**
  * Creates a table that will store information about shortest paths discovered in the graph between
  * two given vertex types.
  */
case class PathSearch(pathRelation: VirtualPathRelation,
                      graph: Graph,
                      catalog: Catalog,
                      sparkSession: SparkSession)
  extends target_api.PathSearch(pathRelation, graph, catalog) {

  override val bindingTable: BindingTableMetadata = {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    val fromData: DataFrame =
      physGraph.tableMap(fromTableName).asInstanceOf[Table[DataFrame]].data
    val toData: DataFrame =
      physGraph.tableMap(toTableName).asInstanceOf[Table[DataFrame]].data
    val pathData: DataFrame = addEdgeData(buildPaths(pathRelation.pathExpression.get))

    logger.info("\nPaths found for expression:\n")
    pathRelation.pathExpression.get.printTree()
    pathData.show()
    fromData.createOrReplaceGlobalTempView(fromTableName.value)
    toData.createOrReplaceGlobalTempView(toTableName.value)
    pathData.createOrReplaceGlobalTempView(s"vpath_${pathRelation.ref.refName}")

    val pathRef: String = pathBinding.refName
    val fromRef: String = fromBinding.refName
    val toRef: String = toBinding.refName
    val fromTableRef: String = fromTableName.value
    val toTableRef: String = toTableName.value

    //TODO Aldana: use a generated label instead of just 'Path'
    val selectPath: String =
      s"""
      SELECT 'Path' AS `$pathRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependOptional(pathData, pathRelation.ref, s"edge$$")}
      FROM global_temp.vpath_${pathRelation.ref.refName}"""

    val addLabelFrom: String =
      s"""
      SELECT "$fromTableRef" AS `$fromRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(fromData, fromBinding)} FROM global_temp.$fromTableRef"""

    val addLabelTo: String =
      s"""
      SELECT "$toTableRef" AS `$toRef$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(toData, toBinding)} FROM global_temp.$toTableRef"""

    val joinPathOnFrom: String =
      s"""
      SELECT * FROM ($selectPath) INNER JOIN ($addLabelFrom) ON
      `${pathRef}_edge$$${FROM_ID_COL.columnName}` = `$fromRef$$${ID_COL.columnName}`"""

    val joinPathOnFromAndTo: String =
      s"""
      SELECT * FROM ($joinPathOnFrom) INNER JOIN ($addLabelTo) ON
      `${pathRef}_edge$$${TO_ID_COL.columnName}` = `$toRef$$${ID_COL.columnName}`"""

    val newPathSchema: StructType = refactorScanSchema(pathData.schema, pathRelation.ref)
    val newFromSchema: StructType = refactorScanSchema(fromData.schema, pathRelation.fromRel.ref)
    val newToSchema: StructType = refactorScanSchema(toData.schema, pathRelation.toRel.ref)

    SqlBindingTableMetadata(
      sparkSchemaMap = Map(
        pathRelation.ref -> newPathSchema,
        pathRelation.fromRel.ref -> newFromSchema,
        pathRelation.toRel.ref -> newToSchema),
      sparkBtableSchema = mergeSchemas(newPathSchema, newFromSchema, newToSchema),
      btableOps = SqlQuery(resQuery = joinPathOnFromAndTo))
  }

  def mergeUdf = udf((leftArray: mutable.WrappedArray[Int],
                      rightArray: mutable.WrappedArray[Int]) =>
    leftArray ++ rightArray
  )

  def castArrayUdf = udf((value: Int) =>
    Array(value)
  )

  def flattenUdf = udf((value: mutable.WrappedArray[mutable.WrappedArray[Int]]) =>
    value.flatten
  )

  def emptyArrayUdf = udf((value: Int) =>
    Array.empty[Int]
  )

  def addUdf = udf((leftValue: Int, rightValue: Int) =>
    leftValue + rightValue
  )

  def arrayJoinUdf = udf((leftValue: mutable.WrappedArray[String], rightValue: String) =>
    leftValue.contains(rightValue)
  )

  //TODO Aldana: create a class PathBindingTable
  def buildPaths(pathExpression: PathExpression):DataFrame = {

    pathExpression match {
      case SimpleKleeneStar(DisjunctLabels(Seq(labels)), 1, 1)  =>
        val edgeData: DataFrame = physGraph.tableMap(labels).asInstanceOf[Table[DataFrame]].data
        edgeData.withColumn("edges", castArrayUdf(col("id")))
          .withColumn("cost", lit(1))
          .select("fromId", "toId", "edges", "cost")
      case KleeneConcatenation(lhs, rhs) =>
        val leftData: DataFrame = buildPaths(lhs)
          .withColumnRenamed("toId", "joinCol")
        val rightData: DataFrame = buildPaths(rhs)
          .withColumnRenamed("fromId", "joinCol")
          .withColumnRenamed("edges", "edgesTmp")
          .withColumnRenamed("cost", "costTmp")
        leftData.join(rightData, "joinCol")
          .withColumn("edges", mergeUdf(col("edges"), col("edgesTmp")))
          .withColumn("cost", addUdf(col("cost"), col("costTmp")))
          .select("fromId", "toId", "edges", "cost")
      case KleeneUnion(lhs, rhs) =>
        val leftData: DataFrame = buildPaths(lhs)
        val rightData: DataFrame = buildPaths(rhs)
        leftData.union(rightData)
      case KleenePlus(exp) =>
        val data: DataFrame = buildPaths(exp)
        buildPathsGraphX(data)
      case SimpleKleenePlus(DisjunctLabels(Seq(labels))) =>
        val edges: DataFrame = physGraph.tableMap(labels).asInstanceOf[Table[DataFrame]].data
        val data:DataFrame = edges.withColumn("edges", castArrayUdf(col("id")))
          .select("fromId", "toId", "edges")
        buildPathsGraphX(data)
      case KleeneStar(exp,_,_) =>
        val data: DataFrame = buildPaths(exp)
        var nodes: DataFrame = physGraph.vertexData.head.asInstanceOf[Table[DataFrame]].data
          .select("id")
        physGraph.vertexData.drop(1).foreach(table => {
          nodes.union(table.asInstanceOf[Table[DataFrame]].data
            .select("id"))
        })
        nodes = nodes.withColumn("fromId", col("id"))
          .withColumn("toId", col("id"))
          .withColumn("edges", emptyArrayUdf(col("id")))
          .withColumn("cost", lit(0))
          .select("fromId", "toId", "edges", "cost")
        buildPathsGraphX(data)
          .union(nodes)
      case SimpleKleeneStar(DisjunctLabels(Seq(labels)), _, _) =>
        val edges: DataFrame = physGraph.tableMap(labels).asInstanceOf[Table[DataFrame]].data
        val data:DataFrame = edges.withColumn("edges", castArrayUdf(col("id")))
          .select("fromId", "toId", "edges")
        var nodes: DataFrame = physGraph.vertexData.head.asInstanceOf[Table[DataFrame]].data
          .select("id")
        physGraph.vertexData.drop(1).foreach(table => {
          nodes.union(table.asInstanceOf[Table[DataFrame]].data
            .select("id"))
        })
        nodes = nodes.withColumn("fromId", col("id"))
          .withColumn("toId", col("id"))
          .withColumn("edges", emptyArrayUdf(col("id")))
          .withColumn("cost", lit(0))
          .select("fromId", "toId", "edges", "cost")
        buildPathsGraphX(data).union(nodes)
      case Wildcard() =>
        var edges: DataFrame = physGraph.edgeData.head.asInstanceOf[Table[DataFrame]].data
            .select("id", "fromId", "toId")
        physGraph.edgeData.drop(1).foreach(table => {
          edges = edges.union(table.asInstanceOf[Table[DataFrame]].data
            .select("id", "fromId", "toId"))
        })
        edges.withColumn("edges", castArrayUdf(col("id")))
          .withColumn("cost", lit(1))
          .select("fromId", "toId", "edges", "cost")
      case KleeneNot(DisjunctLabels(Seq(label))) =>
        val edgeLabels = physGraph.edgeData.filterNot(table => {
          table.name == label
        })
        var edges: DataFrame = edgeLabels.head.asInstanceOf[Table[DataFrame]].data
          .select("id", "fromId", "toId")
        edgeLabels.drop(1).foreach(table => {
          edges = edges.union(table.asInstanceOf[Table[DataFrame]].data
            .select("id", "fromId", "toId"))
        })
        edges.withColumn("edges", castArrayUdf(col("id")))
          .withColumn("cost", lit(1))
          .select("fromId", "toId", "edges", "cost")
      case Reverse(DisjunctLabels(Seq(labels))) =>
        val edgeData: DataFrame = physGraph.tableMap(labels).asInstanceOf[Table[DataFrame]].data
        edgeData.withColumn("edges", castArrayUdf(col("id")))
          .withColumnRenamed("fromId", "aux")
          .withColumnRenamed("toId", "fromId")
          .withColumnRenamed("aux", "toId")
          .withColumn("cost", lit(1))
          .select("fromId", "toId", "edges", "cost")
      case KleeneOptional(exp) =>
        var nodes: DataFrame = physGraph.vertexData.head.asInstanceOf[Table[DataFrame]].data
            .select("id")
        physGraph.vertexData.drop(1).foreach(table => {
          nodes.union(table.asInstanceOf[Table[DataFrame]].data
            .select("id"))
        })
        nodes = nodes.withColumn("fromId", col("id"))
            .withColumn("toId", col("id"))
            .withColumn("edges", emptyArrayUdf(col("id")))
            .withColumn("cost", lit(0))
            .select("fromId", "toId", "edges", "cost")
        nodes.union(buildPaths(exp))
    }
  }

  //We need to add the info of all edges in the paths
  //The use of the operator Optional leaves paths with length zero that must be deleted
  def addEdgeData(paths: DataFrame):DataFrame = {
    def joinCols(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }
    val pathsNoZero = paths.filter(col("cost") > 0)
      .withColumn("id", monotonically_increasing_id())

    var edges: DataFrame = physGraph.edgeData.head.asInstanceOf[Table[DataFrame]].data
    edges = edges.withColumn("table_label", lit(physGraph.edgeData.head.name.value))
    physGraph.edgeData.drop(1).foreach(table => {
      //TODO what if edges have different attributes?
      val edgeTable = table.asInstanceOf[Table[DataFrame]].data
        .withColumn("table_label", lit(table.name.value))
      val cols1 = edges.columns.toSet
      val cols2 = edgeTable.columns.toSet
      val colsTotal = cols1 ++ cols2
      edges = edges.select(joinCols(cols1, colsTotal):_*).union(
        edgeTable.select(joinCols(cols2, colsTotal):_*)
      )
    })

    val prefix = s"edge$$"
    val renamedColumns = edges.columns.map(c=> edges(c).as(s"$prefix$c"))
    edges = edges.select(renamedColumns:_*)

    pathsNoZero.join(edges, arrayJoinUdf(pathsNoZero("edges"), edges(prefix+"id")), "left")

  }

  def selectAllPrependOptional(table: DataFrame, ref: Reference, optional: String): String ={
    table.columns
      .map(col =>
        if (col.startsWith(optional)){
          s"`$col` AS `${ref.refName}_$col`"
        }else {
          s"$col AS `${ref.refName}$$$col`"
        }
      )
      .mkString(", ")
  }

  def buildPathsGraphX(graph: DataFrame):DataFrame = {
    val nodeData: DataFrame = graph.select("fromId").
      withColumnRenamed("fromId", "id").
      union(graph.select("toId").
        withColumnRenamed("toId","id"))
      .distinct

    val edgeData: DataFrame = graph.withColumn("id", monotonically_increasing_id())
      .select("id", "fromId", "toId", "edges")

    val pathMap = edgeData.select("id", "edges")
      .withColumnRenamed("edges","expandedEdges")

    val graphXPaths: DataFrame = createPathData(edgeData,
      nodeData,
      nodeData,
      sparkSession)

    //TODO Aldana: this operation is SLOW. It separates an array column into multiple rows,
    // makes a join with another table and then a GROUP BY to "rearrange" the arrays.
    // I tried to solve this using a Map and a UDF operation on the table but it didnt work
    graphXPaths
      .withColumn("edges", explode(col("edges")))
      .join(pathMap, col("edges") === col("id"))
      .select("fromId", "toId", "expandedEdges")
      .groupBy(col("fromId"), col("toId"))
      .agg(
        flattenUdf(collect_list("expandedEdges")) as "edges"
      )
      .withColumn("cost", size(col("edges")))

  }
}
