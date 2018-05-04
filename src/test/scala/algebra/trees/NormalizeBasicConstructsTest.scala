package algebra.trees

import algebra.expressions._
import algebra.operators.{BasicConstructClause, CondConstructClause}
import algebra.types._
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class NormalizeBasicConstructsTest extends FunSuite with Matchers {

  test("Coalesce BasicConstructs - (a) => {(a)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(Seq(vertexConstruct("a"))),
        whens = Seq(True))
    val expectedConstructRefs = Set(Set("a"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a), (b) => {(a)}; {(b)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(vertexConstruct("a")),
          Seq(vertexConstruct("b"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("a"), Set("b"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a), (a) => {(a)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(vertexConstruct("a")),
          Seq(vertexConstruct("a"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("a"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a), (a GROUP ...) => {(a), (a GROUP ...)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(vertexConstruct("a")),
          Seq(
            vertexConstruct(
              reference = "a",
              groupDeclaration =
                Some(GroupDeclaration(Seq(PropertyRef(Reference("x"), PropertyKey("prop")))))))
        ),
        whens = Seq(True, True))

    val actualCondConstructClause = NormalizeBasicConstructs rewriteTree algebraTree
    assert(actualCondConstructClause.children.size == 1) // one BasicConstructClause
    val actualBasicConstruct =
      actualCondConstructClause.children.head.asInstanceOf[BasicConstructClause]
    assert(actualBasicConstruct.constructPattern.topology.size == 2) // two VertexConstruct
  }

  test("Coalesce BasicConstructs - (a)->(b) => {(a)->(b)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e", "a", "b"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("e", "a", "b"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a)->(b), (a)->(b) => {(a)->(b)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e", "a", "b")),
          Seq(edgeConstruct("e", "a", "b"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("e", "a", "b"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - " +
    "(a)->(b), (a GROUP ...)->(b) => {(a)->(b), (a GROUP ...)->(b)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e", "a", "b")),
          Seq(
            edgeConstruct(
              edgeReference = "e", leftReference = "a", rightReference = "b",
              leftGroupDeclaration =
                Some(GroupDeclaration(Seq(PropertyRef(Reference("x"), PropertyKey("prop")))))
            ))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("e", "a", "b"), Set("e", "a", "b"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a)->(b), (c) => {(a)->(b)}; {(c)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e", "a", "b")),
          Seq(vertexConstruct("c"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("e", "a", "b"), Set("c"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a)->(b), (a) => {(a)->(b), (a)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e", "a", "b")),
          Seq(vertexConstruct("a"))),
        whens = Seq(True, True))
    val actualCondConstructClause = NormalizeBasicConstructs rewriteTree algebraTree
    assert(actualCondConstructClause.children.size == 1) // one BasicConstructClause
    val actualBasicConstructClause =
      actualCondConstructClause.children.head.asInstanceOf[BasicConstructClause]
    val topology = actualBasicConstructClause.constructPattern.topology
    assert(topology.size == 2) // two connection constructs
  }

  test("Coalesce BasicConstructs - (a)->(b), (c)->(d) => {(a)->(b)}; {(c)->(d)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e0", "a", "b")),
          Seq(edgeConstruct("e1", "c", "d"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("e0", "a", "b"), Set("e1", "c", "d"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - (a)->(b), (b)->(c) => {(a)->(b), (b)->(c)}") {
    val algebraTree =
      condConstructClause(
        topologies = Seq(
          Seq(edgeConstruct("e0", "a", "b")),
          Seq(edgeConstruct("e1", "b", "c"))),
        whens = Seq(True, True))
    val expectedConstructRefs = Set(Set("e0", "a", "b", "e1", "c"))
    val actualConstructRefs = collectRefSets(NormalizeBasicConstructs rewriteTree algebraTree)
    assert(actualConstructRefs == expectedConstructRefs)
  }

  test("Coalesce BasicConstructs - " +
    "(a)->(b)->(c) WHEN True, (a)->(b)->(d) WHEN False, (d)->(e) WHEN True, (f)->(g) WHEN False, " +
    "(h)->(i) WHEN True, (i)->(c) WHEN False => " +
    "{(a)->(b)->(c), (b)->(d), (d)->(e), (h)->(i)->(c)}; {(f)->(g)}") {

    val algebraTree =
      condConstructClause(
        topologies = Seq(
          // (a) -> (b) -> (c)
          Seq(edgeConstruct("e0", "a", "b"), edgeConstruct("e1", "b", "c")),
          // (a) -> (b) -> (d)
          Seq(edgeConstruct("e2", "a", "b"), edgeConstruct("e3", "b", "d")),
          // (d) -> (e)
          Seq(edgeConstruct("e4", "d", "e")),
          // (f) -> (g)
          Seq(edgeConstruct("e5", "f", "g")),
          // (h) -> (i)
          Seq(edgeConstruct("e6", "h", "i")),
          // (i) -> (c)
          Seq(edgeConstruct("e7", "i", "c"))
        ),
        whens = Seq(True, False, True, False, True, False))
    val actual = NormalizeBasicConstructs rewriteTree algebraTree

    val expectedConstructRefs: Set[Set[String]] =
      Set(
        Set("a", "b", "c", "d", "e", "h", "i", "e0", "e1", "e2", "e3", "e4", "e6", "e7"),
        Set("f", "g", "e5"))
    val actualConstructRefs = collectRefSets(actual)
    assert(actualConstructRefs == expectedConstructRefs)

    val expectedSortedConditions = Set(Seq("True", "False").sorted, Seq("False"))
    val basicConstructClauses = actual.children
    val actualSortedConditions: Set[Seq[String]] =
      basicConstructClauses.map(basicConstructClause => {
        val when: AlgebraExpression = basicConstructClause.asInstanceOf[BasicConstructClause].when
        flattenExpressionOfBooleansToSortedSeq(when)
      }).toSet
    assert(actualSortedConditions == expectedSortedConditions)
  }

  private def vertexConstruct(reference: String,
                              groupDeclaration: Option[GroupDeclaration] = None)
  : VertexConstruct = {

    VertexConstruct(
      ref = Reference(reference),
      copyPattern = None,
      groupDeclaration = groupDeclaration,
      expr = ObjectConstructPattern.empty)
  }

  private def edgeConstruct(edgeReference: String,
                            leftReference: String,
                            rightReference: String,
                            leftGroupDeclaration: Option[GroupDeclaration] = None,
                            rightGroupDeclaration: Option[GroupDeclaration] = None)
  : EdgeConstruct = {

    EdgeConstruct(
      connName = Reference(edgeReference),
      connType = OutConn,
      leftEndpoint = vertexConstruct(leftReference, leftGroupDeclaration),
      rightEndpoint = vertexConstruct(rightReference, rightGroupDeclaration),
      copyPattern = None,
      groupDeclaration = None,
      expr = ObjectConstructPattern.empty)
  }

  private def condConstructClause(topologies: Seq[Seq[ConnectionConstruct]],
                                  whens: Seq[AlgebraExpression]): CondConstructClause = {
    CondConstructClause(
      (topologies zip whens)
          .map {
            case (topology, when) => BasicConstructClause(ConstructPattern(topology), when)
          }
    )
  }

  private def collectRefSets(condConstructClause: AlgebraTreeNode): Set[Set[String]] = {
    condConstructClause.children
      .map(basicConstructClause => {
        val constructPattern: ConstructPattern =
          basicConstructClause.asInstanceOf[BasicConstructClause].constructPattern
        constructPattern.topology.foldLeft(Set[String]()) {
          case (agg, vc: VertexConstruct) => agg + vc.ref.refName
          case (agg, ec: EdgeConstruct) =>
            agg ++ Set(
              ec.connName.refName, ec.leftEndpoint.getRef.refName, ec.rightEndpoint.getRef.refName)
        }
      })
      .toSet
  }

  private def flattenExpressionOfBooleansToSortedSeq(and: AlgebraExpression): Seq[String] = {
    val flattened: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()
    and.forEachDown {
      case _: And =>
      case True => flattened += "True"
      case False => flattened += "False"
      case other => fail(s"${other.name} unexpected in WHEN sub-tree for this test suite")
    }
    flattened.sorted
  }
}
