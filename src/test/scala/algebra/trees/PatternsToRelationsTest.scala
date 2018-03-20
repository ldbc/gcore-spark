package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}

import scala.collection.mutable

class PatternsToRelationsTest extends FunSuite with Matchers with Inside {

//  test("bindingTable (v)") {
//    val vertex =
//      Vertex(
//        vertexRef = Reference("v"),
//        expr = ObjectPattern(labelsPred = True(), propsPred = True())
//      )
//    val actual = (GcoreToJoinGraph rewriteTree vertex).asInstanceOf[RelationLike]
//
//    assert(actual.getBindingTable.bindingSet == Set(Reference("v")))
//    inside (actual.getBindingTable.relations(Reference("v")).head) {
//      case VertexRelation(Reference("v"), Select(AllRelations(), True(), _), refSet) =>
//        val vBinding: RelationLike = refSet.relations(Reference("v")).head
//        vBinding should matchPattern {
//          case Select(AllRelations(), True(), _) =>
//        }
//    }
//  }
//
//  test("bindingTable (v)-[e]->(w)") {
//    val edge =
//      Edge(
//        connName = Reference("e"),
//        leftEndpoint =
//          Vertex(
//            vertexRef = Reference("v"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//        rightEndpoint =
//          Vertex(
//            vertexRef = Reference("w"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//        connType = OutConn(),
//        expr = ObjectPattern(labelsPred = True(), propsPred = True())
//      )
//    val actual = (GcoreToJoinGraph rewriteTree edge).asInstanceOf[RelationLike]
//    val bindingTable = actual.getBindingTable
//
//    assert(bindingTable.bindingSet == Set(Reference("v"), Reference("e"), Reference("w")))
//
//    bindingTable.relations(Reference("v")).head should matchPattern {
//      case VertexRelation(Reference("v"), Select(AllRelations(), True(), _), _) =>
//    }
//
//    bindingTable.relations(Reference("w")).head should matchPattern {
//      case VertexRelation(Reference("w"), Select(AllRelations(), True(), _), _) =>
//    }
//
//    bindingTable.relations(Reference("e")).head should matchPattern {
//      case EdgeRelation(Reference("e"), Select(AllRelations(), True(), _), _) =>
//    }
//  }
//
//  test("bindingTable (v:Foo|Bar)") {
//    val vertex =
//      Vertex(
//        vertexRef = Reference("v"),
//        expr = ObjectPattern(
//          labelsPred = WithLabels(And(HasLabel(Seq(Label("Foo"), Label("Bar"))), True())),
//          propsPred = True())
//      )
//    val actual = (GcoreToJoinGraph rewriteTree vertex).asInstanceOf[RelationLike]
//    val bindingTable = actual.getBindingTable
//
//    val labels: mutable.ArrayBuffer[Label] = new mutable.ArrayBuffer
//    bindingTable.relations(Reference("v")).foreach(relation => {
//      inside (relation) {
//        case VertexRelation(Reference("v"), Select(Relation(label), True(), _), _) =>
//          labels += label
//      }
//    })
//
//    assert(labels.toSet == Set(Label("Foo"), Label("Bar")))
//  }
//
//  test("Vertex") {
//    val vertex =
//      Vertex(
//        vertexRef = Reference("v"),
//        expr = ObjectPattern(labelsPred = True(), propsPred = True())
//      )
//    val actual = GcoreToJoinGraph rewriteTree vertex
//
//    actual should matchPattern {
//      case VertexRelation(Reference("v"), RelationLike.empty, _) =>
//    }
//  }
//
//  test("Edge (v)-[e]->(w)") {
//    val edge =
//      Edge(
//        connName = Reference("e"),
//        leftEndpoint =
//          Vertex(
//            vertexRef = Reference("v"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//        rightEndpoint =
//          Vertex(
//            vertexRef = Reference("w"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//        connType = OutConn(),
//        expr =
//          ObjectPattern(
//            labelsPred = WithLabels(And(HasLabel(Seq(Label("e_label"))), True())),
//            propsPred = True())
//      )
//    val actual = GcoreToJoinGraph rewriteTree edge
//
//    inside (actual) {
//      case
//        ej @ EquiJoin(
//          EquiJoin(edgeRel, fromRel, GcoreToJoinGraph.idAttr, GcoreToJoinGraph.fromIdAttr, _),
//          toRel,
//          GcoreToJoinGraph.idAttr,
//          GcoreToJoinGraph.toIdAttr,
//          _) =>
//
//        edgeRel should matchPattern {
//          case EdgeRelation(Reference("e"), RelationLike.empty, _) =>
//        }
//
//        fromRel should matchPattern {
//          case VertexRelation(Reference("v"), RelationLike.empty, _) =>
//        }
//
//        toRel should matchPattern {
//          case VertexRelation(Reference("w"), RelationLike.empty, _) =>
//        }
//    }
//  }
//
//  test("Edge (v)<-[e]-(w)") {
//    val edge =
//      Edge(
//        connName = Reference("e"),
//        leftEndpoint =
//          Vertex(
//            vertexRef = Reference("v"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//        rightEndpoint =
//          Vertex(
//            vertexRef = Reference("w"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//        connType = InConn(),
//        expr = ObjectPattern(labelsPred = True(), propsPred = True())
//      )
//    val actual = GcoreToJoinGraph rewriteTree edge
//
//    inside(actual) {
//      case
//        ej @ EquiJoin(
//          EquiJoin(edgeRel, fromRel, GcoreToJoinGraph.idAttr, GcoreToJoinGraph.fromIdAttr, _),
//          toRel,
//          GcoreToJoinGraph.idAttr,
//          GcoreToJoinGraph.toIdAttr,
//          _) =>
//
//        edgeRel should matchPattern {
//          case EdgeRelation(Reference("e"), RelationLike.empty, _) =>
//        }
//
//        fromRel should matchPattern {
//          case VertexRelation(Reference("w"), RelationLike.empty, _) =>
//        }
//
//        toRel should matchPattern {
//          case VertexRelation(Reference("v"), RelationLike.empty, _) =>
//        }
//    }
//  }
//
//  test("GraphPattern Vertex") {
//    val graphPattern =
//      GraphPattern(
//        topology = Seq(
//            Vertex(
//            vertexRef = Reference("v"),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True()))
//        )
//      )
//    val actual = GcoreToJoinGraph rewriteTree graphPattern
//
//    inside (actual) {
//      case r @ VertexRelation(_, _, _) =>
//        assert(r.getBindingTable.bindingSet == Set(Reference("v")))
//    }
//  }
//
//  test("GraphPattern (v)-[e1]->(w)-[e2]->(t)") {
//    val graphPattern =
//      GraphPattern(
//        topology = Seq(
//          Edge(
//            connName = Reference("e1"),
//            leftEndpoint =
//              Vertex(
//                vertexRef = Reference("v"),
//                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//            rightEndpoint =
//              Vertex(
//                vertexRef = Reference("w"),
//                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//            connType = OutConn(),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())
//          ),
//          Edge(
//            connName = Reference("e2"),
//            leftEndpoint =
//              Vertex(
//                vertexRef = Reference("w"),
//                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//            rightEndpoint =
//              Vertex(
//                vertexRef = Reference("t"),
//                expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//            connType = OutConn(),
//            expr = ObjectPattern(labelsPred = True(), propsPred = True())
//          )
//        )
//      )
//    val actual = GcoreToJoinGraph rewriteTree graphPattern
//
//    inside (actual) {
//      case foj @ FullOuterJoin(_, _, _) =>
//        assert(foj.getBindingTable.bindingSet ==
//          Set(Reference("v"), Reference("w"), Reference("t"), Reference("e1"), Reference("e2")))
//        assert(foj.commonInSeenBindingSets == Set(Reference("w")))
//    }
//  }
//
//  test("SimpleMatchClause") {
//    val simpleMatchClause =
//      SimpleMatchClause(
//        graphPattern =
//          GraphPattern(
//            topology = Seq(
//              Vertex(
//                vertexRef = Reference("v"),
//                expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//        graph = NamedGraph("some_graph"))
//    val actual = GcoreToJoinGraph rewriteTree simpleMatchClause
//
//    actual should matchPattern {
//      case SimpleMatchRelation(_, SimpleMatchRelationContext(NamedGraph("some_graph")), _) =>
//    }
//  }
//
//  test("CondMatchClause of disjoint SimpleMatchClauses: (v), (w)") {
//    val condMatchClause =
//      CondMatchClause(
//        simpleMatches = Seq(
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("v"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph()),
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("w"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph())
//        ),
//        where = True()
//      )
//    val actual = GcoreToJoinGraph rewriteTree condMatchClause
//
//    inside (actual) {
//      case s @ Select(CartesianProduct(_, _, _), True(), _) =>
//        assert(s.getBindingTable.bindingSet == Set(Reference("v"), Reference("w")))
//    }
//  }
//
//  test("CondMatchClause of intersecting SimpleMatchClauses: (v), (v)-[e]->(w)") {
//    val condMatchClause =
//      CondMatchClause(
//        simpleMatches = Seq(
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("v"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph()),
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Edge(
//                    connName = Reference("e"),
//                    leftEndpoint =
//                      Vertex(
//                        vertexRef = Reference("v"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//                    rightEndpoint =
//                      Vertex(
//                        vertexRef = Reference("w"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//                    connType = OutConn(),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())
//                  ))),
//            graph = DefaultGraph())
//        ),
//        where = True()
//      )
//    val actual = GcoreToJoinGraph rewriteTree condMatchClause
//
//    inside (actual) {
//      case s @ Select(FullOuterJoin(_, _, _), True(), _) =>
//        assert(s.getBindingTable.bindingSet == Set(Reference("v"), Reference("w"), Reference("e")))
//    }
//  }
//
//  test("CondMatchClause of intersecting SimpleMatchClauses: (v), (v)-[e]->(w), (w)") {
//    val condMatchClause =
//      CondMatchClause(
//        simpleMatches = Seq(
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("v"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph()),
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Edge(
//                    connName = Reference("e"),
//                    leftEndpoint =
//                      Vertex(
//                        vertexRef = Reference("v"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//                    rightEndpoint =
//                      Vertex(
//                        vertexRef = Reference("w"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//                    connType = OutConn(),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())
//                  ))),
//            graph = DefaultGraph()),
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("w"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph())
//        ),
//        where = True()
//      )
//    val actual = GcoreToJoinGraph rewriteTree condMatchClause
//
//    inside (actual) {
//      case s @ Select(FullOuterJoin(_, _, _), True(), _) =>
//        assert(s.getBindingTable.bindingSet == Set(Reference("v"), Reference("w"), Reference("e")))
//    }
//  }
//
//  test("CondMatchClause of intersecting and disjoint SimpleMatchClauses: (v), (v)-[e]->(w), (t)") {
//    val condMatchClause =
//      CondMatchClause(
//        simpleMatches = Seq(
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("v"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph()),
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Edge(
//                    connName = Reference("e"),
//                    leftEndpoint =
//                      Vertex(
//                        vertexRef = Reference("v"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//                    rightEndpoint =
//                      Vertex(
//                        vertexRef = Reference("w"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())),
//                    connType = OutConn(),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())
//                  ))),
//            graph = DefaultGraph()),
//          SimpleMatchClause(
//            graphPattern =
//              GraphPattern(
//                topology = Seq(
//                  Vertex(
//                    vertexRef = Reference("t"),
//                    expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//            graph = DefaultGraph())
//        ),
//        where = True()
//      )
//    val actual = GcoreToJoinGraph rewriteTree condMatchClause
//
//    // Given that we use a Set to keep track of the relations we should join, it is infeasible to
//    // test exact order of the relations in the join. We test the composition of the binding set to
//    // substitute for this inconvenience.
//    inside (actual) {
//      case s @ Select(CartesianProduct(nj @ FullOuterJoin(_, _, _), _, _), True(), _) =>
//        assert(nj.getBindingTable.bindingSet == Set(Reference("v"), Reference("w"), Reference("e")))
//        assert(s.getBindingTable.bindingSet ==
//          Set(Reference("v"), Reference("w"), Reference("e"), Reference("t")))
//    }
//  }
//
//  test("MatchClause") {
//    val matchClause =
//      MatchClause(
//        nonOptMatches =
//          CondMatchClause(
//            simpleMatches = Seq(
//              SimpleMatchClause(
//                graphPattern =
//                  GraphPattern(
//                    topology = Seq(
//                      Vertex(
//                        vertexRef = Reference("v"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//                graph = DefaultGraph())
//            ),
//            where = True()),
//        optMatches = Seq(
//          CondMatchClause(
//            simpleMatches = Seq(
//              SimpleMatchClause(
//                graphPattern =
//                  GraphPattern(
//                    topology = Seq(
//                      Vertex(
//                        vertexRef = Reference("w"),
//                        expr = ObjectPattern(labelsPred = True(), propsPred = True())))),
//                graph = DefaultGraph())
//            ),
//            where = True()
//          ))
//      )
//    val actual = GcoreToJoinGraph rewriteTree matchClause
//
//    inside (actual) {
//      case loj @ LeftOuterJoin(_, _, _) =>
//        assert(loj.getBindingTable.bindingSet == Set(Reference("v"), Reference("w")))
//    }
//  }
}
