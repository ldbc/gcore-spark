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

import algebra.expressions.{Exists, Label, ObjectPattern, PropertyKey, Reference}
import algebra.operators.{MatchClause, SimpleMatchClause}
import algebra.types._

import scala.collection.mutable

/**
  * An analysis phase which maps variable [[Reference]]s to the [[Graph]] in which they will be
  * further matched. The mapping is done as follows:
  * - for the variables used in a [[SimpleMatchClause]] we use the [[Graph]] parameter of that
  * [[SimpleMatchClause]]. This case fits the variables in non-optional and optional
  * [[SimpleMatchClause]]s;
  * - for variables in the [[Exists]] clauses that have not yet been mapped, we try to identify the
  * [[Graph]] based on the [[ObjectPattern]] of the [[Connection]] or [[Connection]]'s endpoints.
  *
  * It is possible that not all the variables in the [[MatchClause]] will be present in the final
  * mapping. If a variable is missing, it means that the analysis could not infer the [[Graph]] it
  * will be matched on.
  */
case class MapBindingToGraph(context: AlgebraContext) {

  def mapBindingToGraph(tree: AlgebraTreeNode): Map[Reference, Graph] = {
    val simpleMatchClauseMapping: Map[Reference, Graph] = bindingsInSimpleMatchClause(tree)
    val existsMapping: Map[Reference, Graph] = bindingsInExists(tree, simpleMatchClauseMapping)

    simpleMatchClauseMapping ++ existsMapping
  }

  /**
    * Traverses the algebraic tree and, for each [[SimpleMatchClause]], adds the mapping from its
    * reference (and possibly endpoint references) to the graph it's defined on.
    */
  private def bindingsInSimpleMatchClause(tree: AlgebraTreeNode): Map[Reference, Graph] = {
    val buffer: mutable.ArrayBuffer[(Reference, Graph)] =
      new mutable.ArrayBuffer[(Reference, Graph)]()
    tree.forEachDown {
      case SimpleMatchClause(graphPattern, graph) =>
        graphPattern.children foreach {
          case v: SingleEndpointConn => buffer += Tuple2(v.getRef, graph)
          case ep: DoubleEndpointConn =>
            buffer += Tuple2(ep.getLeftEndpoint.getRef, graph)
            buffer += Tuple2(ep.getRef, graph)
            buffer += Tuple2(ep.getRightEndpoint.getRef, graph)
        }
      case _ =>
    }

    // TODO: We should check here that no binding is mapped to more than one graph.
    buffer.toMap
  }


  /**
    * Traverses the algebraic tree and, for variables in [[Exists]] sub-clauses that have not yet
    * been mapped, tries to find a mapping to a [[NamedGraph]] based on the [[ObjectPattern]] of
    * the [[Connection]] or the [[Connection]]'s endpoints.
    */
  private def bindingsInExists(tree: AlgebraTreeNode,
                               knownMapping: Map[Reference, Graph]): Map[Reference, Graph] = {
    val buffer: mutable.ArrayBuffer[(Reference, Graph)] =
      new mutable.ArrayBuffer[(Reference, Graph)]()

    tree.forEachDown {
      case Exists(graphPattern) =>
        graphPattern.children.foreach {
          // If a given binding has not been already mapped, we try to map it based on its
          // labels or properties within the ObjectPattern.
          case c: Connection if !knownMapping.isDefinedAt(c.getRef) =>
            c.getExpr.forEachDown {
              case l: Label =>
                context.catalog.allGraphs.foreach(graph => {
                  if (graph.vertexSchema.labels.contains(l))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.edgeSchema.labels.contains(l))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.pathSchema.labels.contains(l))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                })

              case pk: PropertyKey =>
                context.catalog.allGraphs.foreach(graph => {
                  if (graph.vertexSchema.properties.contains(pk))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.edgeSchema.properties.contains(pk))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.pathSchema.properties.contains(pk))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                })

              case _ =>
            }
        }

      case _ =>
    }

    // TODO: We should check here that no binding is mapped to more than one graph.
    buffer.toMap
  }
}
