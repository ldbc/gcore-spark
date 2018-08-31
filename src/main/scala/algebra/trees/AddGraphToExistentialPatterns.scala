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

import algebra.exceptions.AmbiguousGraphForExistentialPatternException
import algebra.expressions.{Exists, Label, ObjectPattern, Reference}
import algebra.operators.SimpleMatchClause
import algebra.types._
import common.trees.TopDownRewriter

/**
  * Rewriting phase that transforms the [[GraphPattern]]s in [[Exists]] sub-queries into
  * [[SimpleMatchClause]]s containing the same [[GraphPattern]] and a [[Graph]].
  *
  * Each [[Connection]] in a [[GraphPattern]] becomes one [[SimpleMatchClause]]. The [[Graph]] of
  * the [[SimpleMatchClause]] will then be the [[Graph]] into which the [[Connection]] in that
  * [[SimpleMatchClause]] should be matched.
  *
  * The [[Graph]] of a [[SingleEndpointConn]]ection is determined as follows:
  * - if the [[Connection]]'s variable is present in the [[AlgebraContext.bindingToGraph]] mapping,
  * then we use that value;
  * - else it will be the [[DefaultGraph]].
  *
  * In case of a [[DoubleEndpointConn]]ection, the [[Graph]] is determined as follows:
  * - first, for each variable in the [[Connection]] (the endpoints and the vertex or path variable)
  * we either assign the [[Graph]] in the [[AlgebraContext.bindingToGraph]] mapping, if there is
  * any, else we assign the [[DefaultGraph]];
  * - we then reduce the sequence of three [[Graph]]s with the rule: a [[NamedGraph]] and a
  * [[DefaultGraph]] become the [[NamedGraph]]; the [[DefaultGraph]] and the [[DefaultGraph]]
  * remain the [[DefaultGraph]]; for two [[NamedGraph]]s, if they represent the same [[Graph]], then
  * they remain the common [[NamedGraph]], otherwise an exception is thrown, since in this case we
  * cannot correctly infer which of the two [[NamedGraph]]s should be chosen.
  */
case class AddGraphToExistentialPatterns(context: AlgebraContext)
  extends TopDownRewriter[AlgebraTreeNode] {

  assert(context.bindingToGraph.isDefined,
    "The bindingToGraph field of the AlgebraContext must be set for this phase.")

  val bindingToGraph: Map[Reference, Graph] = context.bindingToGraph.get

  override val rule: RewriteFuncType = {
    case existsClause: Exists =>
      val simpleMatches: Seq[SimpleMatchClause] =
        existsClause.graphPattern.children
          .map {
            case v: SingleEndpointConn =>
              Tuple2(v, bindingToGraph.getOrElse(v.getRef, DefaultGraph))

            case ep: DoubleEndpointConn =>
              val leftEndpGraph: Graph =
                bindingToGraph.getOrElse(key = ep.getLeftEndpoint.getRef, default = DefaultGraph)
              val rightEndpGraph: Graph =
                bindingToGraph.getOrElse(key = ep.getRightEndpoint.getRef, default = DefaultGraph)
              val connGraph: Graph =
                bindingToGraph.getOrElse(key = ep.getRef, default = DefaultGraph)

              val epGraph: Graph =
                Seq(leftEndpGraph, rightEndpGraph, connGraph)
                  .reduceLeft((graph1: Graph, graph2: Graph) => {
                    (graph1, graph2) match {
                      case (ng: NamedGraph, DefaultGraph) => ng
                      case (DefaultGraph, ng: NamedGraph) => ng
                      case (ng1: NamedGraph, ng2: NamedGraph) =>
                        if (ng1.graphName != ng2.graphName)
                          throw AmbiguousGraphForExistentialPatternException(
                            ng1.graphName, ng2.graphName, ep)
                        else ng1
                      case _ => DefaultGraph
                    }
                  })

              Tuple2(ep, epGraph)
          }
          .map(connToGraph => SimpleMatchClause(GraphPattern(Seq(connToGraph._1)), connToGraph._2))

      existsClause.children = simpleMatches
      existsClause
  }
}
