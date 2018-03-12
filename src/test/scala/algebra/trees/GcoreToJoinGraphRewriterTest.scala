package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}

class GcoreToJoinGraphRewriterTest extends FunSuite with Matchers with Inside {

  test("Vertex") {
    val vertex =
      Vertex(
        vertexRef = Reference("v"),
        expr = ObjectPattern(labelsPred = True(), propsPred = True())
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree vertex

    inside (actual) {
      case vr @ EntityRelation(Reference("v"), _) =>
        assert(vr.getBindings.bindings == Set(Reference("v")))
    }
  }

  test("Edge (v)-[e]->(w)") {
    val edge =
      Edge(
        connName = Reference("e"),
        leftEndpoint =
          Vertex(
            vertexRef = Reference("v"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        rightEndpoint =
          Vertex(
            vertexRef = Reference("w"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        connType = OutConn(),
        expr =
          ObjectPattern(
            labelsPred = WithLabels(And(HasLabel(Seq(Label("e_label"))), True())),
            propsPred = True())
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree edge

    inside (actual) {
      case
        ej @ EquiJoin(
          EquiJoin(edgeRelation, fromRelation, Reference("fromId"), Reference("id"), _),
          toRelation,
          Reference("toId"),
          Reference("id"),
          _) =>

        assert(ej.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))

        edgeRelation should matchPattern {
          case EntityRelation(_, _) =>
        }

        fromRelation should matchPattern {
          case EntityRelation(_, _) =>
        }

        toRelation should matchPattern {
          case EntityRelation(_, _) =>
        }
    }
  }

  test("Edge (v)<-[e]-(w)") {
    val edge =
      Edge(
        connName = Reference("e"),
        leftEndpoint =
          Vertex(
            vertexRef = Reference("v"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        rightEndpoint =
          Vertex(
            vertexRef = Reference("w"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        connType = InConn(),
        expr = ObjectPattern(labelsPred = True(), propsPred = True())
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree edge

    inside(actual) {
      case
        ej @ EquiJoin(
          EquiJoin(edgeRelation, fromRelation, Reference("fromId"), Reference("id"), _),
          toRelation,
          Reference("toId"),
          Reference("id"),
          _) =>

        assert(ej.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))

        edgeRelation should matchPattern {
          case EntityRelation(Reference("e"), _) =>
        }

        fromRelation should matchPattern {
          case EntityRelation(Reference("w"), _) =>
        }

        toRelation should matchPattern {
          case EntityRelation(Reference("v"), _) =>
        }
    }
  }

  test("Edge (v)<-[e]->(w)") {
    val edge =
      Edge(
        connName = Reference("e"),
        leftEndpoint =
          Vertex(
            vertexRef = Reference("v"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        rightEndpoint =
          Vertex(
            vertexRef = Reference("w"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        connType = InOutConn(),
        expr = ObjectPattern(labelsPred = True(), propsPred = True())
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree edge

    inside (actual) {
      case u@UnionAll(_, _, _) =>
        assert(u.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))
    }
  }

  test("Edge (v)-[e]-(w)") {
    val edge =
      Edge(
        connName = Reference("e"),
        leftEndpoint =
          Vertex(
            vertexRef = Reference("v"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        rightEndpoint =
          Vertex(
            vertexRef = Reference("w"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
        connType = UndirectedConn(),
        expr = ObjectPattern(labelsPred = True(), propsPred = True())
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree edge

    inside(actual) {
      case u @ UnionAll(_, _, _) =>
        assert(u.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))
    }
  }

  test("GraphPattern Vertex") {
    val graphPattern =
      GraphPattern(
        topology = Seq(
            Vertex(
            vertexRef = Reference("v"),
            expr = ObjectPattern(labelsPred = True(), propsPred = True()))
        )
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree graphPattern

    inside (actual) {
      case r @ EntityRelation(_, _) =>
        assert(r.getBindings.bindings == Set(Reference("v")))
    }
  }

  test("GraphPattern (v)-[e1]->(w)-[e2]->(t)") {
    val graphPattern =
      GraphPattern(
        topology = Seq(
          Edge(
            connName = Reference("e1"),
            leftEndpoint =
              Vertex(
                vertexRef = Reference("v"),
                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
            rightEndpoint =
              Vertex(
                vertexRef = Reference("w"),
                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
            connType = OutConn(),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())
          ),
          Edge(
            connName = Reference("e2"),
            leftEndpoint =
              Vertex(
                vertexRef = Reference("w"),
                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
            rightEndpoint =
              Vertex(
                vertexRef = Reference("t"),
                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
            connType = OutConn(),
            expr = ObjectPattern(labelsPred = True(), propsPred = True())
          )
        )
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree graphPattern

    inside (actual) {
      case nj @ InnerJoin(_, _, _) =>
        assert(nj.getBindings.bindings ==
          Set(Reference("v"), Reference("w"), Reference("t"), Reference("e1"), Reference("e2")))
        assert(nj.commonInSeenBindingSets.bindings == Set(Reference("w")))
    }
  }

  test("SimpleMatchClause") {
    val simpleMatchClause =
      SimpleMatchClause(
        graphPattern =
          GraphPattern(
            topology = Seq(
              Vertex(
                vertexRef = Reference("v"),
                expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
        graph = NamedGraph("some_graph"))
    val actual = GcoreToJoinGraphRewriter rewriteTree simpleMatchClause

    actual should matchPattern {
      case SimpleMatchRelation(_, SimpleMatchRelationContext(NamedGraph("some_graph")), _) =>
    }
  }

  test("CondMatchClause of disjoint SimpleMatchClauses: (v), (w)") {
    val condMatchClause =
      CondMatchClause(
        simpleMatches = Seq(
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("v"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph()),
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("w"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph())
        ),
        where = True()
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree condMatchClause

    inside (actual) {
      case s @ Select(CartesianProduct(_, _, _), True(), _) =>
        assert(s.getBindings.bindings == Set(Reference("v"), Reference("w")))
    }
  }

  test("CondMatchClause of intersecting SimpleMatchClauses: (v), (v)-[e]->(w)") {
    val condMatchClause =
      CondMatchClause(
        simpleMatches = Seq(
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("v"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph()),
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Edge(
                    connName = Reference("e"),
                    leftEndpoint =
                      Vertex(
                        vertexRef = Reference("v"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
                    rightEndpoint =
                      Vertex(
                        vertexRef = Reference("w"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
                    connType = OutConn(),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())
                  ))),
            graph = DefaultGraph())
        ),
        where = True()
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree condMatchClause

    inside (actual) {
      case s @ Select(InnerJoin(_, _, _), True(), _) =>
        assert(s.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))
    }
  }

  test("CondMatchClause of intersecting SimpleMatchClauses: (v), (v)-[e]->(w), (w)") {
    val condMatchClause =
      CondMatchClause(
        simpleMatches = Seq(
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("v"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph()),
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Edge(
                    connName = Reference("e"),
                    leftEndpoint =
                      Vertex(
                        vertexRef = Reference("v"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
                    rightEndpoint =
                      Vertex(
                        vertexRef = Reference("w"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
                    connType = OutConn(),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())
                  ))),
            graph = DefaultGraph()),
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("w"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph())
        ),
        where = True()
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree condMatchClause

    inside (actual) {
      case s @ Select(InnerJoin(_, _, _), True(), _) =>
        assert(s.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))
    }
  }

  test("CondMatchClause of intersecting and disjoint SimpleMatchClauses: (v), (v)-[e]->(w), (t)") {
    val condMatchClause =
      CondMatchClause(
        simpleMatches = Seq(
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("v"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph()),
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Edge(
                    connName = Reference("e"),
                    leftEndpoint =
                      Vertex(
                        vertexRef = Reference("v"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
                    rightEndpoint =
                      Vertex(
                        vertexRef = Reference("w"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
                    connType = OutConn(),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())
                  ))),
            graph = DefaultGraph()),
          SimpleMatchClause(
            graphPattern =
              GraphPattern(
                topology = Seq(
                  Vertex(
                    vertexRef = Reference("t"),
                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
            graph = DefaultGraph())
        ),
        where = True()
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree condMatchClause

    // Given that we use a Set to keep track of the relations we should join, it is infeasible to
    // test exact order of the relations in the join. We test the composition of the binding set to
    // substitute for this inconvenience.
    inside (actual) {
      case s @ Select(CartesianProduct(nj @ InnerJoin(_, _, _), _, _), True(), _) =>
        assert(nj.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))
        assert(s.getBindings.bindings ==
          Set(Reference("v"), Reference("w"), Reference("e"), Reference("t")))
    }
  }

  test("MatchClause") {
    val matchClause =
      MatchClause(
        nonOptMatches =
          CondMatchClause(
            simpleMatches = Seq(
              SimpleMatchClause(
                graphPattern =
                  GraphPattern(
                    topology = Seq(
                      Vertex(
                        vertexRef = Reference("v"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
                graph = DefaultGraph())
            ),
            where = True()),
        optMatches = Seq(
          CondMatchClause(
            simpleMatches = Seq(
              SimpleMatchClause(
                graphPattern =
                  GraphPattern(
                    topology = Seq(
                      Vertex(
                        vertexRef = Reference("w"),
                        expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
                graph = DefaultGraph())
            ),
            where = True()
          ))
      )
    val actual = GcoreToJoinGraphRewriter rewriteTree matchClause

    inside (actual) {
      case loj @ LeftOuterJoin(_, _, _) =>
        assert(loj.getBindings.bindings == Set(Reference("v"), Reference("w")))
    }
  }
}
