package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.types.{ConnectionType, GroupDeclaration, OutConn}
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

  class GroupConstructUnboundGroupedVertex(reference: Reference,
                                           groupingProps: Seq[PropertyRef],
                                           aggregateFunctions: Seq[PropertySet],
                                           objectConstructPattern: ObjectConstructPattern,
                                           setClause: Option[SetClause],
                                           removeClause: Option[RemoveClause],
                                           when: AlgebraExpression)
    extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case
            GroupConstruct(
              /*baseConstructTable =*/ Select(BindingTable(_), `when`, _),
              /*vertexConstructTable =*/
                ConstructRelation(
                  `reference`,
                  /*isMatchedRef =*/ false,
                  /*relation =*/ AddColumn(
                    `reference`,
                    GroupBy(
                      `reference`,
                      /*relation =*/ BaseConstructTable(baseConstructViewName1, _),
                      /*groupingAttributes =*/ Seq(GroupDeclaration(`groupingProps`)),
                      `aggregateFunctions`,
                      /*having =*/ None)),
                  /*groupedAttributes =*/ `groupingProps`,
                  /*expr =*/ `objectConstructPattern`,
                  `setClause`,
                  `removeClause`),
              /*baseConstructViewName =*/ baseConstructViewName2,
              /*vertexConstructViewName =*/ _,
              /*edgeConstructTable =*/ RelationLike.empty,
              /*createRules =*/ Seq(VertexCreate(`reference`))
            ) =>
            baseConstructViewName1 == baseConstructViewName2
          case _ => false
        },
        s"${left.name} did not match correct pattern. Found:\n${left.treeString()}",
        s"${left.name} matched correct pattern"
      )
    }
  }

  class GroupConstructUnboundVertex(reference: Reference,
                                    objectConstructPattern: ObjectConstructPattern,
                                    setClause: Option[SetClause],
                                    removeClause: Option[RemoveClause],
                                    when: AlgebraExpression) extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case
            GroupConstruct(
              /*baseConstructTable =*/ Select(BindingTable(_), `when`, _),
              /*vertexConstructTable =*/
                ConstructRelation(
                  `reference`,
                  /*isMatchedRef =*/ false,
                  /*relation =*/ AddColumn(
                    `reference`,
                    BaseConstructTable(baseConstructViewName1, _)),
                    /*groupedAttributes =*/ Seq(),
                    /*expr =*/ `objectConstructPattern`,
                    `setClause`,
                    `removeClause`),
              /*baseConstructViewName =*/ baseConstructViewName2,
              /*vertexConstructViewName =*/ _,
              /*edgeConstructTable =*/ RelationLike.empty,
              /*createRules =*/ Seq(VertexCreate(`reference`))
            ) =>
            baseConstructViewName1 == baseConstructViewName2
          case _ => false
        },
        s"${left.name} did not match correct pattern. Found:\n${left.treeString()}",
        s"${left.name} matched correct pattern"
      )
    }
  }

  class GroupConstructBoundVertex(reference: Reference,
                                  objectConstructPattern: ObjectConstructPattern,
                                  setClause: Option[SetClause],
                                  removeClause: Option[RemoveClause],
                                  when: AlgebraExpression) extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case
            GroupConstruct(
              /*baseConstructTable =*/ Select(BindingTable(_), `when`, _),
              /*vertexConstructTable =*/
                Project(
                  /*relation =*/ ConstructRelation(
                    `reference`,
                    /*isMatchedRef =*/ true,
                    /*relation =*/ GroupBy(
                      `reference`,
                      /*relation =*/ BaseConstructTable(baseConstructViewName1, _),
                      /*groupingAttributes =*/ Seq(`reference`),
                      /*aggregateFunctions =*/ Seq(),
                      /*having =*/ None),
                    /*groupedAttributes =*/ Seq(),
                    /*expr =*/ `objectConstructPattern`,
                    `setClause`,
                    `removeClause`),
                  /*attributes =*/ projectAttrs),
              /*baseConstructViewName =*/ baseConstructViewName2,
              /*vertexConstructViewName =*/ _,
              /*edgeConstructTable =*/ RelationLike.empty,
              /*createRules =*/ Seq(VertexCreate(`reference`))
            ) =>
            projectAttrs == Set(reference) && baseConstructViewName1 == baseConstructViewName2
          case _ => false
        },
        s"${left.name} did not match correct pattern. Found:\n${left.treeString()}",
        s"${left.name} matched correct pattern"
      )
    }
  }

  def matchGroupConstructUnboundGroupedVertex(reference: Reference,
                                              groupingProps: Seq[PropertyRef],
                                              aggregateFunctions: Seq[PropertySet] = Seq.empty,
                                              objectConstructPattern: ObjectConstructPattern =
                                                ObjectConstructPattern.empty,
                                              setClause: Option[SetClause] = None,
                                              removeClause: Option[RemoveClause] = None,
                                              when: AlgebraExpression = True)
  : GroupConstructUnboundGroupedVertex =

    new GroupConstructUnboundGroupedVertex(
      reference, groupingProps, aggregateFunctions, objectConstructPattern,
      setClause, removeClause, when)

  def matchGroupConstructUnboundVertex(reference: Reference,
                                       objectConstructPattern: ObjectConstructPattern =
                                        ObjectConstructPattern.empty,
                                       setClause: Option[SetClause] = None,
                                       removeClause: Option[RemoveClause] = None,
                                       when: AlgebraExpression = True)
  : GroupConstructUnboundVertex = {

    new GroupConstructUnboundVertex(
      reference, objectConstructPattern, setClause, removeClause, when)
  }

  def matchGroupConstructBoundVertex(reference: Reference,
                                     objectConstructPattern: ObjectConstructPattern =
                                      ObjectConstructPattern.empty,
                                     setClause: Option[SetClause] = None,
                                     removeClause: Option[RemoveClause] = None,
                                     when: AlgebraExpression = True): GroupConstructBoundVertex = {
    new GroupConstructBoundVertex(reference, objectConstructPattern, setClause, removeClause, when)
  }
}

object CustomMatchers extends CustomMatchers