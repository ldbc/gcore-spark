package spark.graphx

import algebra.operators.Column.{EDGE_SEQ_COL, FROM_ID_COL, ID_COL, TO_ID_COL}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.graphx.ShortestPaths.{EdgeId, VertexInfoMap}

object Utils {
  def COST_COL: String = "path_cost"
  def SP_INFO_COL: String = "sp_info"

  /**
    * Creates a [[DataFrame]] containing shortest path information for all shortest paths between
    * each source vertex and each destination vertex. In the resulting table, each row will contain
    * all the columns of the source vertex, all the columns of a reachable destination vertex, one
    * column storing the path cost (aliased [[COST_COL]]) and one column storing the edge sequence
    * along the path (aliased [[EDGE_SEQ_COL]]).
    */
  def createPathData(edgeData: DataFrame, fromData: DataFrame, toData: DataFrame,
                     sparkSession: SparkSession): DataFrame = {
    // We only need the ids of the vertices in the computation. We use the id as the attribute of
    // the vertex, although the computation will not need it. We add all the available vertices to
    // the graph, both source and destination.
    val vertexRDD: RDD[(VertexId, VertexId)] =
      fromData
        .select(col(ID_COL.columnName)).union(toData.select(col(ID_COL.columnName)))
        .select(col(ID_COL.columnName).cast("long"))
        .rdd
        .map { case Row(id: VertexId) => (id, id) }
    // We use the edge id as the edge attribute, because we need it to compute the sequence of edges
    // along the path.
    val edgeRDD: RDD[Edge[EdgeId]] =
      edgeData
        .select(
          col(ID_COL.columnName).cast("long"),
          col(FROM_ID_COL.columnName).cast("long"),
          col(TO_ID_COL.columnName).cast("long"))
        .rdd
        .map {
          case Row(edgeId: Long, fromId: Long, toId: Long) => Edge(fromId, toId, edgeId)
        }
    val graph: Graph[Long, EdgeId] = Graph(vertexRDD, edgeRDD)

    // TODO: Can we avoid collect here?
    val landmarks: Array[Long] =
      toData
        .select(col(ID_COL.columnName).cast("long"))
        .collect
        .map(_.getLong(0))

    val graphWithShortestPaths: Graph[VertexInfoMap, EdgeId] = ShortestPaths.run(graph, landmarks)

    sparkSession
      // Create a DataFrame from the RDD backing the vertices of the graph.
      .createDataFrame(
        graphWithShortestPaths.mapVertices((_, vertexInfoMap) => vertexInfoMap.toSeq).vertices)
      // We use "id" and "sp_info" for column names.
      .toDF(FROM_ID_COL.columnName, SP_INFO_COL)
      // As "sp_info" contains a list of tuples, we create a new row in the DataFrame for each
      // tuple in the sequence. We keep the original id of the vertex.
      .select(col(FROM_ID_COL.columnName), explode(col(SP_INFO_COL)).as(SP_INFO_COL))
      // We select the id of the vertex as the source id of the path, the landmark id as the
      // destination id of the path, the cost of the path and the edge sequence.
      .select(
        col(FROM_ID_COL.columnName),
        col(s"$SP_INFO_COL._1").as(TO_ID_COL.columnName),
        col(s"$SP_INFO_COL._2._1").as(COST_COL),
        col(s"$SP_INFO_COL._2._2").as(EDGE_SEQ_COL.columnName))
      // The result also contains paths from landmark-to-landmark, of cost 0. We discard them in the
      // final result.
      .where(s"$COST_COL > 0")
  }
}
