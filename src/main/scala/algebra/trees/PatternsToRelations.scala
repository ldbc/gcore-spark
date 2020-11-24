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

import algebra.expressions._
import algebra.operators._
import algebra.types._
import common.exceptions.UnsupportedOperation
import common.trees.BottomUpRewriter

/**
  * A rewriting phase that creates [[VertexRelation]]s from [[Vertex]] nodes, [[EdgeRelation]]s from
  * [[Edge]] nodes, [[StoredPathRelation]]s from [[Path]] nodes with [[Path.isObj]] set to true and
  * [[VirtualPathRelation]]s from [[Path]] nodes with [[Path.isObj]] set to false.
  */
object PatternsToRelations extends BottomUpRewriter[AlgebraTreeNode] {

  private val vertex: RewriteFuncType = {
    case Vertex(ref, objPattern) => objPattern match {
      case omp @ ObjectPattern(True, _) =>
        VertexRelation(
          ref = ref,
          labelRelation = AllRelations,
          expr = objPattern.children.last.asInstanceOf[AlgebraExpression])

      case _ =>
        val disjLabels: DisjunctLabels = objPattern.children.head.asInstanceOf[DisjunctLabels]
        VertexRelation(
          ref = ref,
          labelRelation = Relation(label = disjLabels.children.head.asInstanceOf[Label]),
          expr = objPattern.children.last.asInstanceOf[AlgebraExpression])
    }
  }

  private val edge: RewriteFuncType = {
    case e @ Edge(ref, _, _, connType, objPattern) =>
      val leftEndpointRel: VertexRelation = e.children(1).asInstanceOf[VertexRelation]
      val rightEndpointRel: VertexRelation = e.children(2).asInstanceOf[VertexRelation]
      val edgeRel: RelationLike = objPattern match {
        case omp @ ObjectPattern(True, _) => AllRelations
        case _ =>
          val disjLabels: DisjunctLabels = objPattern.children.head.asInstanceOf[DisjunctLabels]
          Relation(label = disjLabels.children.head.asInstanceOf[Label])
      }
      val expr: AlgebraExpression = objPattern.children.last.asInstanceOf[AlgebraExpression]

      EdgeRelation(
        ref = ref,
        labelRelation = edgeRel,
        expr = expr,
        fromRel = connType match {
          case InConn => rightEndpointRel
          case OutConn => leftEndpointRel
        },
        toRel = connType match {
          case InConn => leftEndpointRel
          case OutConn => rightEndpointRel
        }
      )
  }

  private val path: RewriteFuncType = {
    case p @ Path(
    ref, isReachableTest, _, _, connType, objPattern, quantif, costVarDef, /*isObj =*/ true, _) =>
      val leftEndpointRel: VertexRelation = p.children(1).asInstanceOf[VertexRelation]
      val rightEndpointRel: VertexRelation = p.children(2).asInstanceOf[VertexRelation]
      val pathRel: RelationLike = objPattern match {
        case ObjectPattern(True, _) => AllRelations
        case _ =>
          val disjLabels: DisjunctLabels = objPattern.children.head.asInstanceOf[DisjunctLabels]
          Relation(label = disjLabels.children.head.asInstanceOf[Label])
      }
      val expr: AlgebraExpression = objPattern.children.last.asInstanceOf[AlgebraExpression]

      StoredPathRelation(
        ref,
        isReachableTest,
        labelRelation = pathRel,
        expr,
        fromRel = connType match {
          case InConn => rightEndpointRel
          case OutConn => leftEndpointRel
        },
        toRel = connType match {
          case InConn => leftEndpointRel
          case OutConn => rightEndpointRel
        },
        costVarDef,
        quantif)

    case p @ Path(
    ref, isReachableTest, _, _, connType, _, _, costVarDef, /*isObj =*/ false, pathExpr) =>
      val leftEndpointRel: VertexRelation = p.children(1).asInstanceOf[VertexRelation]
      val rightEndpointRel: VertexRelation = p.children(2).asInstanceOf[VertexRelation]

      VirtualPathRelation(
        ref,
        isReachableTest,
        fromRel = connType match {
          case InConn => rightEndpointRel
          case OutConn => leftEndpointRel
        },
        toRel = connType match {
          case InConn => leftEndpointRel
          case OutConn => rightEndpointRel
        },
        costVarDef,
        pathExpr)
  }

  private val withLabels: RewriteFuncType = {
    case ConjunctLabels(And(_: DisjunctLabels, _: DisjunctLabels)) =>
      throw UnsupportedOperation("Label conjunction is not supported. An entity must have " +
        "only one label associated with it.")
    case ConjunctLabels(And(hl: DisjunctLabels, True)) => hl
    case ConjunctLabels(And(True, hl: DisjunctLabels)) => hl
  }

  override val rule: RewriteFuncType = path orElse edge orElse vertex orElse withLabels
}
