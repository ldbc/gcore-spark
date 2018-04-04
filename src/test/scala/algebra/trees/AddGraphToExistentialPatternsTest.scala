package algebra.trees

import algebra.exceptions.AmbiguousGraphForExistentialPatternException
import algebra.expressions.{Exists, ObjectPattern, Reference, True}
import algebra.operators.SimpleMatchClause
import algebra.types._
import org.scalatest.FunSuite
import schema.GraphDb

class AddGraphToExistentialPatternsTest extends FunSuite {

  private val graph1: NamedGraph = NamedGraph("graph_1")
  private val graph2: NamedGraph = NamedGraph("graph_2")
  private val bindingToGraph: Map[Reference, Graph] =
    Map(Reference("u") -> graph1, Reference("v") -> graph1, Reference("z") -> graph2)

  private val rewriter: AddGraphToExistentialPatterns =
    AddGraphToExistentialPatterns(AlgebraContext(GraphDb.empty, Some(bindingToGraph)))

  private val emptyObjPattern: ObjectPattern = ObjectPattern(True(), True())

  test("Each GraphPattern in Exists becomes a SimpleMatchClause") {
    val conn1 = Vertex(Reference("u"), emptyObjPattern)
    val conn2 = Vertex(Reference("w"), emptyObjPattern)
    val graphPattern = GraphPattern(topology = Seq(conn1, conn2))
    val exists = Exists(graphPattern)

    val expected =
      Set(
        SimpleMatchClause(GraphPattern(Seq(conn1)), graph1),
        SimpleMatchClause(GraphPattern(Seq(conn2)), DefaultGraph())
      )

    val actual = rewriter rewriteTree exists

    assert(actual.children.size == 2)
    assert(actual.children.toSet == expected)
  }

  test("Exists on mapped vertex - (u) in graph_1") {
    val conn = Vertex(Reference("u"), emptyObjPattern)
    runSingleConnTest(conn, graph1)
  }

  test("Exists on un-mapped vertex - (w) in default graph") {
    val conn = Vertex(Reference("w"), emptyObjPattern)
    runSingleConnTest(conn, DefaultGraph())
  }

  test("Exists on mapped edge - (u)->(v) in graph_1") {
    val left = Vertex(Reference("u"), emptyObjPattern)
    val right = Vertex(Reference("v"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn(), emptyObjPattern)
    runSingleConnTest(conn, graph1)
  }

  test("Exists on un-mapped edge - (x)->(y) in default graph") {
    val left = Vertex(Reference("x"), emptyObjPattern)
    val right = Vertex(Reference("y"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn(), emptyObjPattern)
    runSingleConnTest(conn, DefaultGraph())
  }

  test("Exists on edge with one endpoint mapped will take mapped endpoint's graph - " +
    "(u)->(y), (u) in graph_1, (y) in default graph") {
    val left = Vertex(Reference("u"), emptyObjPattern)
    val right = Vertex(Reference("y"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn(), emptyObjPattern)
    runSingleConnTest(conn, graph1)
  }

  test("Exists on edge with differently mapped endpoints will throw error - " +
    "(u)->(z), (u) in graph_1, (z) in graph_2") {
    val left = Vertex(Reference("u"), emptyObjPattern)
    val right = Vertex(Reference("z"), emptyObjPattern)
    val conn = Edge(Reference("e"), left, right, OutConn(), emptyObjPattern)
    val graphPattern = GraphPattern(topology = Seq(conn))
    val exists = Exists(graphPattern)

    assertThrows[AmbiguousGraphForExistentialPatternException] {
      val actual = rewriter rewriteTree exists
    }
  }

  private def runSingleConnTest(conn: Connection, expectedGraph: Graph): Unit = {
    val graphPattern = GraphPattern(topology = Seq(conn))
    val exists = Exists(graphPattern)
    val expected = SimpleMatchClause(graphPattern, expectedGraph)
    val actual = rewriter rewriteTree exists
    assert(actual.children.head == expected)
  }
}
