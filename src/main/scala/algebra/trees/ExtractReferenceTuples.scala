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

package algebra.trees

import algebra.expressions.Reference
import algebra.types.{Edge, Path, Vertex}

import scala.collection.mutable

/**
  * Traverses an algebraic tree and extracts all vertex, edge and path references found in that
  * tree. The result is wrapped into a [[BindingContext]].
  */
object ExtractReferenceTuples {

  def extractReferenceTuples(algebraTreeNode: AlgebraTreeNode): BindingContext = {
    val vertexTuples: mutable.ArrayBuffer[Reference] = mutable.ArrayBuffer[Reference]()
    val edgeTuples: mutable.ArrayBuffer[ReferenceTuple] = mutable.ArrayBuffer[ReferenceTuple]()
    val pathTuples: mutable.ArrayBuffer[ReferenceTuple] = mutable.ArrayBuffer[ReferenceTuple]()

    algebraTreeNode.forEachDown {
      case vertex: Vertex => vertexTuples += vertex.getRef
      case edge: Edge =>
        edgeTuples +=
          ReferenceTuple(edge.getRef, edge.getLeftEndpoint.getRef, edge.getRightEndpoint.getRef)
      case path: Path =>
        pathTuples +=
          ReferenceTuple(path.getRef, path.getLeftEndpoint.getRef, path.getRightEndpoint.getRef)
      case _ =>
    }

    BindingContext(vertexTuples.toSet, edgeTuples.toSet, pathTuples.toSet)
  }
}
