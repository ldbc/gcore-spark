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
