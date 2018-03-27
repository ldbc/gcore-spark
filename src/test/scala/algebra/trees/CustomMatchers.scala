package algebra.trees

import algebra.expressions.{Label, Reference}
import algebra.operators._
import org.scalatest.matchers.{MatchResult, Matcher}

trait CustomMatchers {

  class EdgeMatcher(fromRef: String, fromRel: String,
                    edgeRef: String, edgeRel: String,
                    toRef: String, toRel: String)
    extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case SimpleMatchRelation(
          EdgeRelation(Reference(`edgeRef`), Relation(Label(`edgeRel`)), _,
          /*fromRel =*/ VertexRelation(Reference(`fromRef`), Relation(Label(`fromRel`)), _),
          /*toRel =*/ VertexRelation(Reference(`toRef`), Relation(Label(`toRel`)), _)),
          _, _) => true
          case _ => false
        },
        s"Edge did not match pattern: (:$fromRel)-[:$edgeRel]->[:$toRel]",
        s"Edge matched pattern: (:$fromRel)-[:$edgeRel]->[:$toRel]"
      )
    }
  }

  class PathMatcher(fromRef: String, fromRel: String,
                    pathRef: String, pathRel: String,
                    toRef: String, toRel: String)
    extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case SimpleMatchRelation(
          StoredPathRelation(Reference(`pathRef`), _, Relation(Label(`pathRel`)), _,
          /*fromRel =*/ VertexRelation(Reference(`fromRef`), Relation(Label(`fromRel`)), _),
          /*toRel =*/ VertexRelation(Reference(`toRef`), Relation(Label(`toRel`)), _), _, _),
          _, _) => true
          case _ => false
        },
        s"Path did not match pattern: (:$fromRel)-[:$pathRel]->[:$toRel]",
        s"Path matched pattern: (:$fromRel)-[:$pathRel]->[:$toRel]"
      )
    }
  }

  val matchEdgeFoodMadeInCountry = new EdgeMatcher("f", "Food","e", "MadeIn",  "c", "Country")
  val matchPathCatToGourmandFood = new PathMatcher("c", "Cat", "p", "ToGourmand", "f", "Food")
}

object CustomMatchers extends CustomMatchers