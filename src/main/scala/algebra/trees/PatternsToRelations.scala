package algebra.trees

import algebra.exceptions.UnsupportedOperation
import algebra.expressions._
import algebra.operators._
import algebra.types._
import common.trees.BottomUpRewriter

object PatternsToRelations extends BottomUpRewriter[AlgebraTreeNode] {

  private val vertex: RewriteFuncType = {
    case Vertex(ref, objPattern) => objPattern match {
      case omp @ ObjectPattern(True(), _) =>
        VertexRelation(
          ref = ref,
          labelRelation = AllRelations(),
          expr = objPattern.children.last.asInstanceOf[AlgebraExpression])

      case _ =>
        val hasLabel: HasLabel = objPattern.children.head.asInstanceOf[HasLabel]
        VertexRelation(
          ref = ref,
          labelRelation = Relation(label = hasLabel.children.head.asInstanceOf[Label]),
          expr = objPattern.children.last.asInstanceOf[AlgebraExpression])
    }
  }

  private val edge: RewriteFuncType = {
    case e @ Edge(ref, _, _, connType, objPattern) =>
      val leftEndpointRel: VertexRelation = e.children(1).asInstanceOf[VertexRelation]
      val rightEndpointRel: VertexRelation = e.children(2).asInstanceOf[VertexRelation]
      val edgeRel: RelationLike = objPattern match {
        case omp @ ObjectPattern(True(), _) => AllRelations()
        case _ =>
          val hasLabel: HasLabel = objPattern.children.head.asInstanceOf[HasLabel]
          Relation(label = hasLabel.children.head.asInstanceOf[Label])
      }
      val expr: AlgebraExpression = objPattern.children.last.asInstanceOf[AlgebraExpression]

      EdgeRelation(
        ref = ref,
        labelRelation = edgeRel,
        expr = expr,
        fromRel = connType match {
          case InConn() => rightEndpointRel
          case OutConn() => leftEndpointRel
        },
        toRel = connType match {
          case InConn() => leftEndpointRel
          case OutConn() => rightEndpointRel
        }
      )
  }

  private val path: RewriteFuncType = {
    case p @ Path(
    ref, isReachableTest, _, _, connType, objPattern, quantif, costVarDef, /*isObj =*/ true) =>
      val leftEndpointRel: VertexRelation = p.children(1).asInstanceOf[VertexRelation]
      val rightEndpointRel: VertexRelation = p.children(2).asInstanceOf[VertexRelation]
      val pathRel: RelationLike = objPattern match {
        case omp @ ObjectPattern(True(), _) => AllRelations()
        case _ =>
          val hasLabel: HasLabel = objPattern.children.head.asInstanceOf[HasLabel]
          Relation(label = hasLabel.children.head.asInstanceOf[Label])
      }
      val expr: AlgebraExpression = objPattern.children.last.asInstanceOf[AlgebraExpression]

      StoredPathRelation(
        ref,
        isReachableTest,
        labelRelation = pathRel,
        expr,
        fromRel = connType match {
          case InConn() => rightEndpointRel
          case OutConn() => leftEndpointRel
        },
        toRel = connType match {
          case InConn() => leftEndpointRel
          case OutConn() => rightEndpointRel
        },
        costVarDef,
        quantif)
  }

  private val withLabels: RewriteFuncType = {
    case WithLabels(And(HasLabel(_), HasLabel(_))) =>
      throw UnsupportedOperation("Label conjunction is not supported. An entity must have " +
        "only one label associated with it.")
    case WithLabels(And(hl @ HasLabel(_), True())) => hl
    case WithLabels(And(True(), hl @ HasLabel(_))) => hl
  }

  override val rule: RewriteFuncType = path orElse edge orElse vertex orElse withLabels
}
