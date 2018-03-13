package algebra.trees

import algebra.expressions.{ObjectPattern, Reference, True}
import algebra.operators._
import algebra.types.{Edge, OutConn, Vertex}
import org.scalatest.{FunSuite, Inside, Matchers}

class ExpandDoubleEndpRelationTest extends FunSuite with Matchers with Inside {

  test("EdgeRelation (v)-[e]->(w)") {
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
        expr = ObjectPattern(labelsPred = True(), propsPred = True())
      )
    val joinGraph = GcoreToJoinGraph rewriteTree edge
    val actual = ExpandDoubleEndpRelation rewriteTree joinGraph

    inside (actual) {
      case EdgeRelation(Reference("e"), RelationLike.empty, bindingTable) =>
        assert(bindingTable.bindingSet == Set(Reference("e"), Reference("v"), Reference("w")))

        val eRelInBtable: RelationLike = bindingTable.relations(Reference("e")).head
        val vRelInBtable: RelationLike = bindingTable.relations(Reference("v")).head
        val wRelInBtable: RelationLike = bindingTable.relations(Reference("w")).head

        eRelInBtable should matchPattern {
          case
            SemiJoin(
              SemiJoin(
                EdgeRelation(Reference("e"), Select(AllRelations(), True(), _), _),
                VertexRelation(Reference("v"), Select(AllRelations(), True(), _), _),
                GcoreToJoinGraph.idAttr,
                GcoreToJoinGraph.fromIdAttr,
                _),
              VertexRelation(Reference("w"), Select(AllRelations(), True(), _), _),
              GcoreToJoinGraph.idAttr,
              GcoreToJoinGraph.toIdAttr,
              _) =>
        }

        vRelInBtable should matchPattern {
          case
            SemiJoin(
              VertexRelation(Reference("v"), Select(AllRelations(), True(), _), _),
              SemiJoin(
                SemiJoin(
                  EdgeRelation(Reference("e"), Select(AllRelations(), True(), _), _),
                  VertexRelation(Reference("v"), Select(AllRelations(), True(), _), _),
                  GcoreToJoinGraph.idAttr,
                  GcoreToJoinGraph.fromIdAttr,
                  _),
                VertexRelation(Reference("w"), Select(AllRelations(), True(), _), _),
                GcoreToJoinGraph.idAttr,
                GcoreToJoinGraph.toIdAttr,
                _),
              GcoreToJoinGraph.fromIdAttr,
              GcoreToJoinGraph.idAttr,
              _) =>
        }

        wRelInBtable should matchPattern {
          case
            SemiJoin(
              VertexRelation(Reference("w"), Select(AllRelations(), True(), _), _),
              SemiJoin(
                SemiJoin(
                  EdgeRelation(Reference("e"), Select(AllRelations(), True(), _), _),
                  VertexRelation(Reference("v"), Select(AllRelations(), True(), _), _),
                  GcoreToJoinGraph.idAttr,
                  GcoreToJoinGraph.fromIdAttr,
                  _),
                VertexRelation(Reference("w"), Select(AllRelations(), True(), _), _),
                GcoreToJoinGraph.idAttr,
                GcoreToJoinGraph.toIdAttr,
                _),
              GcoreToJoinGraph.toIdAttr,
              GcoreToJoinGraph.idAttr,
              _) =>
        }
    }
  }
}
