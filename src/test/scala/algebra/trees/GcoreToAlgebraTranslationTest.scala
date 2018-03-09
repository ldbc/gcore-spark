package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}

class GcoreToAlgebraTranslationTest extends FunSuite with Matchers with Inside {

  test("HasLabel one Label") {
    val hasLabel = HasLabel(Seq(Label("foo")))
    val expected = Relation(Label("foo"))
    val actual = GcoreToAlgebraTranslation rewriteTree hasLabel
    assert(actual == expected)
  }

  test("HasLabel multiple Labels") {
    val hasLabel = HasLabel(Seq(Label("foo"), Label("bar"), Label("baz")))
    val expected =
      UnionAll(
        UnionAll(Relation(Label("foo")), Relation(Label("bar"))),
        Relation(Label("baz"))
      )
    val actual = GcoreToAlgebraTranslation rewriteTree hasLabel
    assert(actual == expected)
  }

  test("ObjectPattern WithLabels") {
    val objectPattern =
      ObjectPattern(
        labelsPred = WithLabels(And(HasLabel(Seq(Label("foo"))), True())),
        propsPred = True()
      )
    val expected =
      Select(
        relation = Relation(Label("foo")),
        expr = True(),
        bindingContext = None // No bindings at this level in the tree.
      )
    val actual = GcoreToAlgebraTranslation rewriteTree objectPattern
    assert(actual == expected)
  }

  test("ObjectPattern no WithLabels") {
    val objectPattern =
      ObjectPattern(
        labelsPred = True(),
        propsPred = True()
      )
    val expected =
      Select(
        relation = UnionAllRelations(),
        expr = True(),
        bindingContext = None // No bindings at this level in the tree.
      )
    val actual = GcoreToAlgebraTranslation rewriteTree objectPattern
    assert(actual == expected)
  }

  test("Vertex") {
    val vertex =
      Vertex(
        vertexRef = Reference("v"),
        expr =
          ObjectPattern(
            labelsPred = WithLabels(And(HasLabel(Seq(Label("foo"))), True())),
            propsPred = True())
      )
    val actual = GcoreToAlgebraTranslation rewriteTree vertex

    inside (actual) {
      case
        Projection(
          AttributeSet(Attribute(Reference("id"))),
          Select(_, _, _),
          Some(BindingContext(bset))) =>

        assert(bset.bindings == Set(Reference("v")))
    }
  }

  test("Edge (v)-[e]->(w)") {
    val edge =
      Edge(
        connName = Reference("e"),
        leftEndpoint =
          Vertex(
            vertexRef = Reference("v"),
            expr =
              ObjectPattern(
                labelsPred = WithLabels(And(HasLabel(Seq(Label("v_label"))), True())),
                propsPred = True())),
        rightEndpoint =
          Vertex(
            vertexRef = Reference("w"),
            expr =
              ObjectPattern(
                labelsPred = WithLabels(And(HasLabel(Seq(Label("w_label"))), True())),
                propsPred = True())),
        connType = OutConn(),
        expr =
          ObjectPattern(
            labelsPred = WithLabels(And(HasLabel(Seq(Label("e_label"))), True())),
            propsPred = True())
      )
    val actual = GcoreToAlgebraTranslation rewriteTree edge

    inside (actual) {
      case
        Rename(
          Rename(
            Rename(
              edgeProjection,
              RenameAttribute(
                Attribute(Reference("fromId")),
                Attribute(Reference("v"))),
              _),
            RenameAttribute(
              Attribute(Reference("toId")),
              Attribute(Reference("w"))),
            _),
          RenameAttribute(
            Attribute(Reference("id")),
            Attribute(Reference("e"))),
          Some(BindingContext(bset))) =>

        assert(bset.bindings == Set(Reference("v"), Reference("w"), Reference("e")))

        inside (edgeProjection) {
          case
            Projection(
              AttributeSet(
                Attribute(Reference("id")),
                Attribute(Reference("fromId")),
                Attribute(Reference("toId"))),
              semiJoin,
              _) =>

            inside (semiJoin) {
              case SemiJoin(SemiJoin(edgeRelation, fromRelation, _), toRelation, _) => {
                edgeRelation should matchPattern {
                  case Select(Relation(Label("e_label"), _), _, _) =>
                }

                fromRelation should matchPattern {
                  case
                    Rename(
                      Projection(
                        AttributeSet(Attribute(Reference("id"))),
                        Select(Relation(Label("v_label"), _), _, _),
                        _),
                      RenameAttribute(Attribute(Reference("id")), Attribute(Reference("fromId"))),
                      _) =>
                }

                toRelation should matchPattern {
                  case
                    Rename(
                      Projection(
                        AttributeSet(Attribute(Reference("id"))),
                        Select(Relation(Label("w_label"), _), _, _),
                        _),
                      RenameAttribute(Attribute(Reference("id")), Attribute(Reference("toId"))),
                      _) =>
                }
              }
            }
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
    val actual = GcoreToAlgebraTranslation rewriteTree edge

    inside(actual) {
      case
        Rename(
          Rename(
            Rename(
              _,
              RenameAttribute(Attribute(Reference("fromId")), Attribute(Reference("w"))),
              _),
            RenameAttribute(Attribute(Reference("toId")), Attribute(Reference("v"))),
            _),
          RenameAttribute(Attribute(Reference("id")), Attribute(Reference("e"))),
          Some(BindingContext(bset))) =>

        assert(bset.bindings == Set(Reference("v"), Reference("w"), Reference("e")))
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
    val actual = GcoreToAlgebraTranslation rewriteTree edge

    inside(actual) {
      case u @ UnionAll(vToW, wToV, _) => {
        assert(u.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))

        vToW should matchPattern {
          case
            Rename(
              Rename(
                Rename(
                  _,
                  RenameAttribute(Attribute(Reference("fromId")), Attribute(Reference("v"))),
                  _),
                RenameAttribute(Attribute(Reference("toId")), Attribute(Reference("w"))),
                _),
              RenameAttribute(Attribute(Reference("id")), Attribute(Reference("e"))),
              _) =>
        }

        wToV should matchPattern {
          case
            Rename(
              Rename(
                Rename(
                  _,
                  RenameAttribute(Attribute(Reference("fromId")), Attribute(Reference("w"))),
                  _),
                RenameAttribute(Attribute(Reference("toId")), Attribute(Reference("v"))),
                _),
              RenameAttribute(Attribute(Reference("id")), Attribute(Reference("e"))),
              _) =>
        }
      }
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
    val actual = GcoreToAlgebraTranslation rewriteTree edge

    inside(actual) {
      case u @ UnionAll(vToW, wToV, _) => {
        assert(u.getBindings.bindings == Set(Reference("v"), Reference("w"), Reference("e")))

        vToW should matchPattern {
          case
            Rename(
              Rename(
                Rename(
                  _,
                  RenameAttribute(Attribute(Reference("fromId")), Attribute(Reference("v"))),
                  _),
                RenameAttribute(Attribute(Reference("toId")), Attribute(Reference("w"))),
                _),
              RenameAttribute(Attribute(Reference("id")), Attribute(Reference("e"))),
              _) =>
        }

        wToV should matchPattern {
          case
            Rename(
              Rename(
                Rename(
                  _,
                  RenameAttribute(Attribute(Reference("fromId")), Attribute(Reference("w"))),
                  _),
                RenameAttribute(Attribute(Reference("toId")), Attribute(Reference("v"))),
                _),
              RenameAttribute(Attribute(Reference("id")), Attribute(Reference("e"))),
              _) =>
        }
      }
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
    val actual = GcoreToAlgebraTranslation rewriteTree graphPattern

    inside (actual) {
      case r @ Rename(_, _, _) =>
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
    val actual = GcoreToAlgebraTranslation rewriteTree graphPattern

    inside (actual) {
      case nj @ NaturalJoin(_, _, _) =>
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
    val actual = GcoreToAlgebraTranslation rewriteTree simpleMatchClause

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
    val actual = GcoreToAlgebraTranslation rewriteTree condMatchClause

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
    val actual = GcoreToAlgebraTranslation rewriteTree condMatchClause

    inside (actual) {
      case s @ Select(NaturalJoin(_, _, _), True(), _) =>
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
    val actual = GcoreToAlgebraTranslation rewriteTree condMatchClause

    inside (actual) {
      case s @ Select(NaturalJoin(_, _, _), True(), _) =>
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
    val actual = GcoreToAlgebraTranslation rewriteTree condMatchClause

    // Given that we use a Set to keep track of the relations we should join, it is infeasible to
    // test exact order of the relations in the join. We test the composition of the binding set to
    // substitute for this inconvenience.
    inside (actual) {
      case s @ Select(CartesianProduct(nj @ NaturalJoin(_, _, _), _, _), True(), _) =>
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
    val actual = GcoreToAlgebraTranslation rewriteTree matchClause

    inside (actual) {
      case loj @ LeftOuterJoin(_, _, _) =>
        assert(loj.getBindings.bindings == Set(Reference("v"), Reference("w")))
    }
  }
}
