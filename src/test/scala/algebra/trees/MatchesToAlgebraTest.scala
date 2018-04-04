package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.types.GcoreInteger
import org.scalatest.{BeforeAndAfterAll, FunSuite, Inside, Matchers}
import parser.SpoofaxParser
import parser.trees.ParseContext

import scala.reflect.ClassTag

class MatchesToAlgebraTest extends FunSuite
  with BeforeAndAfterAll with Matchers with Inside with TestGraphWrapper {

  private val spoofaxParser: SpoofaxParser = SpoofaxParser(ParseContext(graphDb))
  private val expandRelations: ExpandRelations = ExpandRelations(AlgebraContext(graphDb))

  override def beforeAll() {
    super.beforeAll()
    graphDb.registerGraph(catsGraph)
    graphDb.setDefaultGraph("cats graph")
  }

  test("CondMatchClause becomes a Select") {
    val query = "CONSTRUCT () MATCH (c:Cat) WHERE c.weight > 4"
    val select = rewrite(query).children.head

    select should matchPattern {
      case Select(
      /*relation =*/ _,
      /*expr =*/ Gt(PropertyRef(Reference("c"), PropertyKey("weight")), Literal(4, GcoreInteger())),
      /*bindingSet =*/ _) =>
    }
  }

  test("Multi-labeled patterns are unioned - (v)->(w) and (v)") {
    val edgeQuery = "CONSTRUCT () MATCH (v)-[e]->(w)"
    val vertexQuery = "CONSTRUCT () MATCH (v)"
    val edgeRelation = extractRelationUnderSelect(edgeQuery)
    val vertexRelation = extractRelationUnderSelect(vertexQuery)

    testSingleBinOp[UnionAll](
      edgeRelation, Set(EdgeOrPathReference(Reference("v"), Reference("e"), Reference("w"))))
    testSingleBinOp[UnionAll](vertexRelation, Set(VertexReference(Reference("v"))))
  }

  test("Patterns with common bindings are inner-joined - (v:Cat)->(w:Food), (v:Cat)") {
    val query = "CONSTRUCT () MATCH (v:Cat)-[e:Eats]->(w:Food), (v:Cat)"
    val relation = extractRelationUnderSelect(query)
    testSingleBinOp[InnerJoin](
      relation,
      Set(
        EdgeOrPathReference(Reference("v"), Reference("e"), Reference("w")),
        VertexReference(Reference("v"))))
  }

  test("Disjoint patterns are cross-joined - (v:Cat)->(w:Food), (u:Cat)") {
    val query = "CONSTRUCT () MATCH (v:Cat)-[e:Eats]->(w:Food), (u:Cat)"
    val relation = extractRelationUnderSelect(query)
    testSingleBinOp[CrossJoin](
      relation,
      Set(
        EdgeOrPathReference(Reference("v"), Reference("e"), Reference("w")),
        VertexReference(Reference("u"))))
  }

  test("Mix of union, inner- and cross- joins - (c1:Cat)->(c2:Cat)->(f:Food), (c3:Country)") {
    val query = "CONSTRUCT () MATCH (c1:Cat)-[e1]->(c2:Cat)-[e2]->(f:Food), (c3:Country)"
    val relation = extractRelationUnderSelect(query)

    inside (relation) {
      case CrossJoin(cjLhs @ InnerJoin(_, _, _), cjRhs, _) =>
        assert(extractRefTuples(Seq(cjRhs)) == Set(VertexReference(Reference("c3"))))

        inside (cjLhs) {
          case InnerJoin(ijLhs, ijRhs, _) =>
            assert(
              extractRefTuples(Seq(ijRhs)) ==
                Set(EdgeOrPathReference(Reference("c2"), Reference("e2"), Reference("f"))))
            testSingleBinOp[UnionAll](
              ijLhs,
              Set(EdgeOrPathReference(Reference("c1"), Reference("e1"), Reference("c2")))
            )
        }
    }
  }

  test("Mix of union, and inner-join in exists sub-clause - (c1:Cat)->(c2:Cat)->(f:Food)") {
    val query = "CONSTRUCT () MATCH () WHERE (c1:Cat)-[e1]->(c2:Cat)-[e2]->(f:Food)"
    val relation = extractRelationUnderExists(query)

    inside (relation) {
      case cjLhs @ InnerJoin(ijLhs, ijRhs, _) =>
        assert(
          extractRefTuples(Seq(ijRhs)) ==
            Set(EdgeOrPathReference(Reference("c2"), Reference("e2"), Reference("f"))))
        testSingleBinOp[UnionAll](
          ijLhs,
          Set(EdgeOrPathReference(Reference("c1"), Reference("e1"), Reference("c2")))
        )
    }
  }

  private def extractRelationUnderSelect(query: String): RelationLike = {
    val select = rewrite(query).children.head
    select.children.head.asInstanceOf[RelationLike]
  }

  private def extractRelationUnderExists(query: String): RelationLike = {
    val select = rewrite(query).children.head
    val exists = select.children.last
    exists.children.head.asInstanceOf[RelationLike]
  }

  private def rewrite(query: String): AlgebraTreeNode = {
    val context = AlgebraContext(graphDb, Some(Map.empty)) // all vars in the default graph
    val treeWithExists: AlgebraTreeNode =
      AddGraphToExistentialPatterns(context).rewriteTree(spoofaxParser.parse(query))
    val treeWithRelations = PatternsToRelations rewriteTree treeWithExists
    val expandedTree = expandRelations rewriteTree treeWithRelations
    MatchesToAlgebra rewriteTree expandedTree
  }

  private
  def testSingleBinOp[T <: BinaryPrimitive: ClassTag]
  (op: RelationLike, expectedRefTuples: Set[ReferenceTuple]): Unit = {

    op shouldBe a [T]

    val simpleMatches: Seq[RelationLike] = extractBinOpOperands[T](op.asInstanceOf[T])
    val actualRefTuples: Set[ReferenceTuple] = extractRefTuples(simpleMatches)

    assert(actualRefTuples == expectedRefTuples)
  }

  private def extractRefTuples(simpleMatches: Seq[RelationLike]): Set[ReferenceTuple] = {
    simpleMatches
      .map(simpleMatch => {
        val rel = simpleMatch.children.head
        rel match {
          case vr: VertexRelation => VertexReference(vr.ref)
          case er: EdgeRelation => EdgeOrPathReference(er.fromRel.ref, er.ref, er.toRel.ref)
          case pr: StoredPathRelation => EdgeOrPathReference(pr.fromRel.ref, pr.ref, pr.toRel.ref)
        }
      })
      .toSet
  }

  private
  def extractBinOpOperands[T <: BinaryPrimitive: ClassTag](op: T): Seq[RelationLike] = {
    val rhs: RelationLike = op.children.last.asInstanceOf[RelationLike]

    op.children.head match {
      case binOp: T => extractBinOpOperands[T](binOp) :+ rhs
      case simpleMatch: SimpleMatchRelation => Seq(simpleMatch, rhs)
      case other => fail(s"Left operand cannot be a(n) ${other.name}")
    }
  }

  sealed abstract class ReferenceTuple
  sealed case class VertexReference(ref: Reference) extends ReferenceTuple
  sealed case class EdgeOrPathReference(fromRef: Reference, ref: Reference, toRef: Reference)
    extends ReferenceTuple
}
