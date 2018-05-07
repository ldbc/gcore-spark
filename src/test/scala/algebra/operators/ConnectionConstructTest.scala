package algebra.operators

import algebra.exceptions.AmbiguousMerge
import algebra.expressions._
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}

class ConnectionConstructTest extends FunSuite with Matchers with Inside {

  test("Conflicting references throw exception - (u) merge (v)") {
    val u = vertexConstruct(reference = Reference("u"))
    val v = vertexConstruct(reference = Reference("v"))
    assertThrows[AmbiguousMerge] { u merge v }
  }

  test("Conflicting endpoint copy patterns throw exception - (u=v) merge (u=w)") {
    val uv = vertexConstruct(reference = Reference("u"), copyPattern = Some(Reference("v")))
    val uw = vertexConstruct(reference = Reference("u"), copyPattern = Some(Reference("w")))
    assertThrows[AmbiguousMerge] { uv merge uw }
  }

  test("Conflicting expressions for the same property throw exception - " +
    "(u {prop := 0}) merge (u {prop := 1})") {
    val prop0 = PropAssignment(PropertyKey("prop"), IntLiteral(0))
    val prop1 = PropAssignment(PropertyKey("prop"), IntLiteral(1))
    val uprop0 =
      vertexConstruct(
        reference = Reference("u"),
        objConstructPattern =
          ObjectConstructPattern(LabelAssignments(Seq.empty), PropAssignments(Seq(prop0)))
      )
    val uprop1 =
      vertexConstruct(
        reference = Reference("u"),
        objConstructPattern =
          ObjectConstructPattern(LabelAssignments(Seq.empty), PropAssignments(Seq(prop1)))
      )
    assertThrows[AmbiguousMerge] { uprop0 merge uprop1 }
  }

  test("Conflicting source and destination for edge merge throw exception - " +
    "(a)-[e]->(b) merge (a)<-[e]-(b), (a)-[e]->(b) merge (a)-[e]->(c), " +
    "(c)-[e]->(b) merge (a)-[e]->(b)") {

    val a = vertexConstruct(Reference("a"))
    val b = vertexConstruct(Reference("b"))
    val c = vertexConstruct(Reference("c"))
    val aToB = edgeConstruct(Reference("e"), OutConn, a, b)
    val bToA = edgeConstruct(Reference("e"), InConn, a, b)
    val aToC = edgeConstruct(Reference("e"), OutConn, a, c)
    val cToB = edgeConstruct(Reference("e"), OutConn, c, b)

    assertThrows[AmbiguousMerge] { aToB merge bToA }
    assertThrows[AmbiguousMerge] { aToB merge aToC }
    assertThrows[AmbiguousMerge] { cToB merge aToB }
  }

  /**
    * Source and destination are a and b, respectively, for both pattern, but the connection type
    * between them is different between the two.
    */
  test("Conflicting connection type for edge - (a)-[e]-(b) merge (a)-[e]->(b)") {
    val a = vertexConstruct(Reference("a"))
    val b = vertexConstruct(Reference("b"))
    val aToB = edgeConstruct(Reference("e"), OutConn, a, b)
    val aUndirB = edgeConstruct(Reference("e"), UndirectedConn, a, b)

    assertThrows[AmbiguousMerge] { aToB merge aUndirB }
  }

  test("Empty vertices merged correctly - (u) merge (u)") {
    val u = vertexConstruct(Reference("u"))
    val merged = u merge u
    u should matchPattern {
      case VertexConstruct(Reference("u"), None, None, ObjectConstructPattern.empty) =>
    }
  }

  test("Common copy pattern is passed to the result - (u=v) merge (u=v)") {
    val u = vertexConstruct(Reference("u"), copyPattern = Some(Reference("v")))
    val merged = u merge u
    u should matchPattern {
      case VertexConstruct(Reference("u"), Some(Reference("v")), _, _) =>
    }
  }

  test("Defined copy pattern is passed to result - (u) merge (u=v), (u=v) merge (u)") {
    val u = vertexConstruct(Reference("u"))
    val uCopy = vertexConstruct(Reference("u"), copyPattern = Some(Reference("v")))
    (u merge uCopy) should matchPattern {
      case VertexConstruct(Reference("u"), Some(Reference("v")), _, _) =>
    }
    (uCopy merge u) should matchPattern {
      case VertexConstruct(Reference("u"), Some(Reference("v")), _, _) =>
    }
  }

  test("Merged GroupDeclaration is passed to the result - " +
    "(u GROUP v.prop0) merge (u GROUP v.prop1") {
    val prop0 = PropertyRef(Reference("v"), PropertyKey("prop0"))
    val prop1 = PropertyRef(Reference("v"), PropertyKey("prop1"))
    val uprop0 =
      vertexConstruct(Reference("u"), groupDeclaration = Some(GroupDeclaration(Seq(prop0))))
    val uprop1 =
      vertexConstruct(Reference("u"), groupDeclaration = Some(GroupDeclaration(Seq(prop1))))
    (uprop0 merge uprop1) should matchPattern {
      case VertexConstruct(Reference("u"), _, Some(GroupDeclaration(Seq(`prop0`, `prop1`))), _) =>
    }
  }

  test("Defined GroupDeclaration is passed to the result - " +
    "(u GROUP v.prop) merge (u), (u) merge (u GROUP v.prop)") {
    val prop = PropertyRef(Reference("v"), PropertyKey("prop"))
    val u = vertexConstruct(Reference("u"))
    val uprop =
      vertexConstruct(Reference("u"), groupDeclaration = Some(GroupDeclaration(Seq(prop))))
    (u merge uprop) should matchPattern {
      case VertexConstruct(Reference("u"), _, Some(GroupDeclaration(Seq(`prop`))), _) =>
    }
    (uprop merge u) should matchPattern {
      case VertexConstruct(Reference("u"), _, Some(GroupDeclaration(Seq(`prop`))), _) =>
    }
  }

  test("Label set is passed to result - (u:Label1:Label2) merge (u:Label2)") {
    val label1 = Label("Label1")
    val label2 = Label("Label2")
    val ulabel1 =
      vertexConstruct(
        Reference("u"),
        objConstructPattern =
          ObjectConstructPattern(LabelAssignments(Seq(label1, label2)), PropAssignments(Seq.empty)))
    val ulabel2 =
      vertexConstruct(
        Reference("u"),
        objConstructPattern =
          ObjectConstructPattern(LabelAssignments(Seq(label2)), PropAssignments(Seq.empty)))
    (ulabel1 merge ulabel2) should matchPattern {
      case
        VertexConstruct(
          Reference("u"), _, _,
          ObjectConstructPattern(LabelAssignments(Seq(`label1`, `label2`)), _)) =>
    }
  }

  test("Merged property set is passed to the result - " +
    "(u {prop0 := 0, prop1 := 1}) merge (u {prop0 := 0, prop2 := 2}") {
    val prop0 = PropAssignment(PropertyKey("prop0"), IntLiteral(0))
    val prop1 = PropAssignment(PropertyKey("prop1"), IntLiteral(1))
    val prop2 = PropAssignment(PropertyKey("prop2"), IntLiteral(2))
    val left =
      vertexConstruct(
        Reference("u"),
        objConstructPattern =
          ObjectConstructPattern(LabelAssignments(Seq.empty), PropAssignments(Seq(prop0, prop1)))
      )
    val right =
      vertexConstruct(
        Reference("u"),
        objConstructPattern =
          ObjectConstructPattern(LabelAssignments(Seq.empty), PropAssignments(Seq(prop1, prop2)))
      )
    inside(left merge right) {
      case
        VertexConstruct(
          Reference("u"), _, _,
          ObjectConstructPattern(_, PropAssignments(propAssignments))) =>

        val expectedPropAssignments = Set(prop0, prop1, prop2)
        assert(propAssignments.size == expectedPropAssignments.size)
        assert(propAssignments.toSet == expectedPropAssignments)
    }
  }

  test("Edge endpoints are merged - (a)-[e]->(b) merge (a {prop0 := 0})-[e]->(b {prop1 := 1})") {
    val prop0 = PropAssignment(PropertyKey("prop0"), IntLiteral(0))
    val prop1 = PropAssignment(PropertyKey("prop1"), IntLiteral(1))
    val a = vertexConstruct(reference = Reference("a"))
    val aprop0 = vertexConstruct(
      reference = Reference("a"),
      objConstructPattern =
        ObjectConstructPattern(
          labelAssignments = LabelAssignments(Seq.empty),
          propAssignments = PropAssignments(Seq(prop0)))
    )
    val b = vertexConstruct(reference = Reference("b"))
    val bprop1 = vertexConstruct(
      reference = Reference("b"),
      objConstructPattern =
        ObjectConstructPattern(
          labelAssignments = LabelAssignments(Seq.empty),
          propAssignments = PropAssignments(Seq(prop1)))
    )
    val e = edgeConstruct(Reference("e"), OutConn, a, b)
    val eprops = edgeConstruct(Reference("e"), OutConn, aprop0, bprop1)

    (e merge eprops) should matchPattern {
      case
        EdgeConstruct(
          Reference("e"), OutConn,
          VertexConstruct(
            Reference("a"), _, _, ObjectConstructPattern(_, PropAssignments(Seq(`prop0`)))),
          VertexConstruct(
            Reference("b"), _, _, ObjectConstructPattern(_, PropAssignments(Seq(`prop1`)))),
          /*copyPattern =*/ None, /*groupDeclaration =*/ None,
          /*objConstructPattern =*/ ObjectConstructPattern.empty) =>
    }
  }

  test("Edge properties are merged - (a)-[e]->(b) merge (a)-[e {prop := 0}]->(b)") {
    val prop = PropAssignment(PropertyKey("prop0"), IntLiteral(0))
    val a = vertexConstruct(reference = Reference("a"))
    val b = vertexConstruct(reference = Reference("b"))
    val e = edgeConstruct(Reference("e"), OutConn, a, b)
    val eprop =
      edgeConstruct(
        reference = Reference("e"),
        connectionType = OutConn,
        leftEndpoint = a,
        rightEndpoint = b,
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments = PropAssignments(Seq(prop)))
      )

    (e merge eprop) should matchPattern {
      case
        EdgeConstruct(
          Reference("e"), OutConn,
          VertexConstruct(Reference("a"), _, _, _),
          VertexConstruct(Reference("b"), _, _, _),
          /*copyPattern =*/ None, /*groupDeclaration =*/ None,
          /*objConstructPattern =*/
          ObjectConstructPattern(
            LabelAssignments(Seq()),
            PropAssignments(Seq(`prop`)))) =>
    }
  }

  private def vertexConstruct(reference: Reference,
                              copyPattern: Option[Reference] = None,
                              groupDeclaration: Option[GroupDeclaration] = None,
                              objConstructPattern: ObjectConstructPattern =
                                ObjectConstructPattern.empty): VertexConstruct = {
    VertexConstruct(reference, copyPattern, groupDeclaration, objConstructPattern)
  }

  private def edgeConstruct(reference: Reference,
                            connectionType: ConnectionType,
                            leftEndpoint: VertexConstruct,
                            rightEndpoint: VertexConstruct,
                            copyPattern: Option[Reference] = None,
                            groupDeclaration: Option[GroupDeclaration] = None,
                            objConstructPattern: ObjectConstructPattern =
                              ObjectConstructPattern.empty): EdgeConstruct = {
    EdgeConstruct(
      reference, connectionType, leftEndpoint, rightEndpoint,
      copyPattern, groupDeclaration, objConstructPattern)
  }
}
