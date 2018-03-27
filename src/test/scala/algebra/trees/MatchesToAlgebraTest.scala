package algebra.trees

import algebra.expressions.Reference
import algebra.operators._
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

  test("Multi-labeled patterns are unioned - (v)->(w) and (v)") {
    val edgeQuery = "CONSTRUCT () MATCH (v)-[e]->(w)"
    val vertexQuery = "CONSTRUCT () MATCH (v)"
    val edgeRelation = extractCondMatchRelation(edgeQuery)
    val vertexRelation = extractCondMatchRelation(vertexQuery)

    testSingleBinOp[UnionAll](
      edgeRelation, Set(EdgeOrPathReference(Reference("v"), Reference("e"), Reference("w"))))
    testSingleBinOp[UnionAll](vertexRelation, Set(VertexReference(Reference("v"))))
  }

  test("Patterns with common bindings are inner-joined - (v:Cat)->(w:Food), (v:Cat)") {
    val query = "CONSTRUCT () MATCH (v:Cat)-[e:Eats]->(w:Food), (v:Cat)"
    val relation = extractCondMatchRelation(query)
    testSingleBinOp[InnerJoin](
      relation,
      Set(
        EdgeOrPathReference(Reference("v"), Reference("e"), Reference("w")),
        VertexReference(Reference("v"))))
  }

  test("Disjoint patterns are cross-joined - (v:Cat)->(w:Food), (u:Cat)") {
    val query = "CONSTRUCT () MATCH (v:Cat)-[e:Eats]->(w:Food), (u:Cat)"
    val relation = extractCondMatchRelation(query)
    testSingleBinOp[CrossJoin](
      relation,
      Set(
        EdgeOrPathReference(Reference("v"), Reference("e"), Reference("w")),
        VertexReference(Reference("u"))))
  }

  test("Mix of union, inner- and cross- joins - (c1:Cat)->(c2:Cat)->(f:Food), (c3:Country)") {
    val query = "CONSTRUCT () MATCH (c1:Cat)-[e1]->(c2:Cat)-[e2]->(f:Food), (c3:Country)"
    val relation = extractCondMatchRelation(query)

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

  private def extractCondMatchRelation(query: String): RelationLike = {
    val condMatch = rewrite(query).children.head
    condMatch.children.head.asInstanceOf[RelationLike]
  }

  private def rewrite(query: String): AlgebraTreeNode = {
    val parseTree = PatternsToRelations rewriteTree spoofaxParser.parse(query)
    val expandedTree = expandRelations rewriteTree parseTree
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
