package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.types._
import org.scalatest.{FunSuite, Matchers}

class PatternsToRelationsTest extends FunSuite with Matchers {

  private val emptyObjPattern: ObjectPattern = ObjectPattern(True, True)
  private val labeledObjPattern: ObjectPattern =
    ObjectPattern(
      labelsPred = ConjunctLabels(And(DisjunctLabels(Seq(Label("foo"))), True)),
      propsPred = True)

  test("Vertex - match on AllRelations() if no label provided") {
    val vertex = Vertex(vertexRef = Reference("v"), expr = emptyObjPattern)
    val relation = PatternsToRelations rewriteTree vertex

    relation should matchPattern {
      case VertexRelation(Reference("v"), AllRelations, _) =>
    }
  }

  test("Vertex - match on Relation(label) if label provided") {
    val vertex = Vertex(vertexRef = Reference("v"), expr = labeledObjPattern)
    val relation = PatternsToRelations rewriteTree vertex

    relation should matchPattern {
      case VertexRelation(Reference("v"), Relation(Label("foo")), _) =>
    }
  }

  test("Edge (v)-[e]->(w) or (v)<-[e]-(w) - match on AllRelations() if no label provided") {
    val from = Vertex(vertexRef = Reference("v"), expr = emptyObjPattern)
    val to = Vertex(vertexRef = Reference("w"), expr = emptyObjPattern)
    val edgeOutConn =
      Edge(
        connName = Reference("e"),
        leftEndpoint = from, rightEndpoint = to,
        connType = OutConn,
        expr = emptyObjPattern)
    val relationOutConn = PatternsToRelations rewriteTree edgeOutConn
    val edgeInConn =
      Edge(
        connName = Reference("e"),
        leftEndpoint = from, rightEndpoint = to,
        connType = InConn,
        expr = emptyObjPattern)
    val relationInConn = PatternsToRelations rewriteTree edgeInConn

    relationOutConn should matchPattern {
      case EdgeRelation(Reference("e"), AllRelations, _,
      /*fromRel = */ VertexRelation(Reference("v"), _, _),
      /*toRel = */ VertexRelation(Reference("w"), _, _)) =>
    }

    relationInConn should matchPattern {
      case EdgeRelation(Reference("e"), AllRelations, _,
      /*fromRel = */ VertexRelation(Reference("w"), _, _),
      /*toRel = */ VertexRelation(Reference("v"), _, _)) =>
    }
  }

  test("Edge (v)-[e]->(w) or (v)<-[e]-(w) - match on Relation(label) if label provided") {
    val from = Vertex(vertexRef = Reference("v"), expr = emptyObjPattern)
    val to = Vertex(vertexRef = Reference("w"), expr = emptyObjPattern)
    val edgeOutConn =
      Edge(
        connName = Reference("e"),
        leftEndpoint = from, rightEndpoint = to,
        connType = OutConn,
        expr = labeledObjPattern)
    val relationOutConn = PatternsToRelations rewriteTree edgeOutConn
    val edgeInConn =
      Edge(
        connName = Reference("e"),
        leftEndpoint = from, rightEndpoint = to,
        connType = InConn,
        expr = labeledObjPattern)
    val relationInConn = PatternsToRelations rewriteTree edgeInConn

    relationOutConn should matchPattern {
      case EdgeRelation(Reference("e"), Relation(Label("foo")), _,
      /*fromRel = */ VertexRelation(Reference("v"), _, _),
      /*toRel = */ VertexRelation(Reference("w"), _, _)) =>
    }

    relationInConn should matchPattern {
      case EdgeRelation(Reference("e"), Relation(Label("foo")), _,
      /*fromRel = */ VertexRelation(Reference("w"), _, _),
      /*toRel = */ VertexRelation(Reference("v"), _, _)) =>
    }
  }

  test("Stored path (v)-/@/->(w) or (v)<-/@/-(w) - match on AllRelations() if no label provided") {
    val from = Vertex(vertexRef = Reference("v"), expr = emptyObjPattern)
    val to = Vertex(vertexRef = Reference("w"), expr = emptyObjPattern)
    val pathOutConn =
      Path(
        connName = Reference("p"),
        isReachableTest = false,
        leftEndpoint = from, rightEndpoint = to,
        connType = OutConn,
        expr = emptyObjPattern,
        quantifier = AllPaths, costVarDef = None, isObj = true, pathExpression = None)
    val relationOutConn = PatternsToRelations rewriteTree pathOutConn
    val pathInConn =
      Path(
        connName = Reference("p"),
        isReachableTest = false,
        leftEndpoint = from, rightEndpoint = to,
        connType = InConn,
        expr = emptyObjPattern,
        quantifier = AllPaths, costVarDef = None, isObj = true, pathExpression = None)
    val relationInConn = PatternsToRelations rewriteTree pathInConn

    relationOutConn should matchPattern {
      case StoredPathRelation(Reference("p"), /*isReachableTest =*/ false, AllRelations, _,
      /*fromRel = */ VertexRelation(Reference("v"), _, _),
      /*toRel = */ VertexRelation(Reference("w"), _, _),
      /*costVarDef =*/ None, /*quantifier = */ AllPaths) =>
    }

    relationInConn should matchPattern {
      case StoredPathRelation(Reference("p"), /*isReachableTest =*/ false, AllRelations, _,
      /*fromRel = */ VertexRelation(Reference("w"), _, _),
      /*toRel = */ VertexRelation(Reference("v"), _, _),
      /*costVarDef =*/ None, /*quantifier = */ AllPaths) =>
    }
  }

  test("Stored path (v)-/@/->(w) or (v)<-/@/-(w) - match on Relations(label) if label provided") {
    val from = Vertex(vertexRef = Reference("v"), expr = emptyObjPattern)
    val to = Vertex(vertexRef = Reference("w"), expr = emptyObjPattern)
    val pathOutConn =
      Path(
        connName = Reference("p"),
        isReachableTest = false,
        leftEndpoint = from, rightEndpoint = to,
        connType = OutConn,
        expr = labeledObjPattern,
        quantifier = AllPaths, costVarDef = None, isObj = true, pathExpression = None)
    val relationOutConn = PatternsToRelations rewriteTree pathOutConn
    val pathInConn =
      Path(
        connName = Reference("p"),
        isReachableTest = false,
        leftEndpoint = from, rightEndpoint = to,
        connType = InConn,
        expr = labeledObjPattern,
        quantifier = AllPaths, costVarDef = None, isObj = true, pathExpression = None)
    val relationInConn = PatternsToRelations rewriteTree pathInConn

    relationOutConn should matchPattern {
      case StoredPathRelation(Reference("p"), /*isReachableTest =*/ false, Relation(Label("foo")),
      /*expr = */ _,
      /*fromRel = */ VertexRelation(Reference("v"), _, _),
      /*toRel = */ VertexRelation(Reference("w"), _, _),
      /*costVarDef =*/ None, /*quantifier = */ AllPaths) =>
    }

    relationInConn should matchPattern {
      case StoredPathRelation(Reference("p"), /*isReachableTest =*/ false, Relation(Label("foo")),
      /*expr = */ _,
      /*fromRel = */ VertexRelation(Reference("w"), _, _),
      /*toRel = */ VertexRelation(Reference("v"), _, _),
      /*costVarDef =*/ None, /*quantifier = */ AllPaths) =>
    }
  }
}
