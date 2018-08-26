package spark.graphx

import org.apache.spark.graphx._

/** Algorithms to discover various flavours of shortest paths within a graphx.[[Graph]]. */
object ShortestPaths {

  type Distance = Int
  type EdgeId = Long

  /**
    * The payload (attribute) of each vertex in the [[Graph]] used for the shortest path discovery.
    * Note that the distance is not necessarily equal to the length of the path (given here as a
    * sequence of edges), as we may be computing weighted shortest paths.
    */
  type VertexInfoMap = Map[VertexId, (Distance, Seq[EdgeId])]

  /**
    * Computes the shortest path from each vertex in the [[Graph]] to each vertex in the set of
    * landmarks. At the end of the computation, the attribute of the vertices in the returned
    * [[Graph]] will be a [[VertexInfoMap]] containing the mapping between each reachable landmark
    * vertex and the distance to that landmark, as well as the sequence of edges traversed to reach
    * that landmark. Each vertex and edge is represented by its id in the original database graph.
    *
    * The algorithm used to compute the shortest paths is more or less the same as that in the
    * original [[org.apache.spark.graphx.lib.ShortestPaths]]:
    * https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala
    * with minor modifications to retrieve the edge sequence.
    */
  def run(graph: Graph[VertexId, EdgeId], landmarks: Seq[Long]): Graph[VertexInfoMap, EdgeId] = {
    // In the beginning, each landmark "knows" the distance to itself is 0. Other vertices have no
    // information in the initial iteration.
    val graphWithInfoMap: Graph[VertexInfoMap, EdgeId] =
      graph.mapVertices {
        case (vertexId, _) =>
          if (landmarks.contains(vertexId)) Map(vertexId -> (0, Seq.empty))
          else Map.empty
      }

    // We run a Pregel-like computation to discover the shortest paths, on the pre-processed graph.
    Pregel(
      graph = graphWithInfoMap,

      // We send an empty initial message.
      initialMsg = Map.empty[VertexId, (Distance, Seq[EdgeId])],

      // At each iteration, sendMsg will be called on both endpoints of an edge where either side
      // received a message at the previous iteration. For example, for the edge (a)-[e]->(b), if
      // b has discovered a shortest path at a previous iteration, it will send itself a message (as
      // per the sendMsg function). Then, at the current iteration, both (a) and (b) will run
      // sendMsg and thus (a) will discover a shortest path through (b) to all the nodes (b) has
      // already discovered.
      activeDirection = EdgeDirection.Either)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMaps)
  }

  /**
    * When a vertex receives a message, it will simply join the two [[VertexInfoMap]]s (its own and
    * the message) to compute its new [[VertexInfoMap]]. Note that this is a message sent by the
    * vertex itself at the previous iteration.
    */
  private def vertexProgram(vertexId: VertexId,
                            vertexInfoMap: VertexInfoMap,
                            msgInfoMap: VertexInfoMap): VertexInfoMap =
    mergeMaps(vertexInfoMap, msgInfoMap)

  /**
    * At this point in the computation, a vertex has a wider local view - it can access its own
    * attribute, as well as its outbound edge's and that edge's destination (the neighbor). The
    * vertex will probe whether by following the edge to its neighbor it can obtain different paths
    * to the landmarks from the ones it already knows. If so, it sends itself a message with the new
    * paths. Note that, upon receiving a message (from itself), the vertex will retain only the
    * shortest path to the landmarks (see [[mergeMaps]]).
    */
  private def sendMessage(edge: EdgeTriplet[VertexInfoMap, EdgeId])
  : Iterator[(VertexId, VertexInfoMap)] = {
    val newAttr: VertexInfoMap = incrementMap(edge.attr, edge.dstAttr) // probes the neighbor
    if (edge.srcAttr != mergeMaps(newAttr, edge.srcAttr))
      Iterator((edge.srcId, newAttr)) // sends message to itself, the source id
    else
      Iterator.empty
  }

  /**
    * A [[VertexInfoMap]] is changed by incrementing the distance with one and prepending the
    * followed edge to the path (the edge sequence).
    */
  private def incrementMap(edgeId: EdgeId, dstInfoMap: VertexInfoMap): VertexInfoMap = {
    dstInfoMap.map {
      case (vertexId, (distance, edgeSeq)) => vertexId -> (distance + 1, edgeId +: edgeSeq)
    }
  }

  /**
    * Merges two [[VertexInfoMap]]s by keeping for each key (a landmark vertex) the smallest
    * distance in either map and its corresponding edge sequence.
    */
  private def mergeMaps(map1: VertexInfoMap, map2: VertexInfoMap): VertexInfoMap = {
    (map1.keySet ++ map2.keySet).map {
      vertexId =>
        val dist1: Distance =
          if (map1.contains(vertexId)) map1(vertexId)._1
          else Int.MaxValue
        val dist2: Distance =
          if (map2.contains(vertexId)) map2(vertexId)._1
          else Int.MaxValue

        if (dist1 < dist2) vertexId -> map1(vertexId)
        else vertexId -> map2(vertexId)
    }.toMap
  }
}
