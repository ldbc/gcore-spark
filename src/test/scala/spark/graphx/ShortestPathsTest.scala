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

package spark.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import spark.SparkSessionTestWrapper
import spark.graphx.ShortestPaths.{Distance, EdgeId, VertexInfoMap}

class ShortestPathsTest extends FunSuite with SparkSessionTestWrapper {

  type SourceId = VertexId
  type DestinationId = VertexId

  test("Shortest paths") {
    val edgeTriplets: Seq[(SourceId, DestinationId, EdgeId)] = Seq(
      (1, 2, 102), (1, 5, 105), (2, 3, 203), (2, 5, 205),
      (3, 4, 304), (4, 5, 405), (4, 6, 406), (5, 2, 502))

    val vertexRDD: RDD[(VertexId, VertexId)] =
      spark.sparkContext.parallelize(
        edgeTriplets.flatMap {
          case (sourceId, destId, _) => Seq((sourceId, sourceId), (destId, destId))
        })
    val edgeRDD: RDD[Edge[EdgeId]] =
      spark.sparkContext.parallelize(
        edgeTriplets.map {
          case (sourceId, destId, edgeId) => Edge(sourceId, destId, edgeId)
        })
    val graph: Graph[VertexId, EdgeId] = Graph(vertexRDD, edgeRDD)

    val landmarks: Seq[VertexId] = Seq(2, 4)

    val expectedPaths: Set[(VertexId, VertexInfoMap)] =
      Seq(
        (1L, Map(2L -> (1, Seq(102L)), 4L -> (3, Seq(102L, 203L, 304L)))),
        (2L, Map(2L -> (0, Seq()), 4L -> (2, Seq(203L, 304L)))),
        (3L, Map(2L -> (3, Seq(304L, 405L, 502L)), 4L -> (1, Seq(304L)))),
        (4L, Map(2L -> (2, Seq(405L, 502L)), 4L -> (0, Seq()))),
        (5L, Map(2L -> (1, Seq(502L)), 4L -> (3, Seq(502L, 203L, 304L)))),
        (6L, Map.empty[VertexId, (Distance, Seq[EdgeId])])
      ).toSet

    val actualPaths: Set[(VertexId, VertexInfoMap)] =
      ShortestPaths.run(graph, landmarks).vertices.collect().toSet

    assert(actualPaths == expectedPaths)
  }
}
