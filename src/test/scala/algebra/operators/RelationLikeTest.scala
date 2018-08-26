package algebra.operators

import algebra.expressions.{Label, Reference, True}
import algebra.types.AllPaths
import org.scalatest.FunSuite

class RelationLikeTest extends FunSuite {

  test("Empty BindingSet for Relation") {
    val relation = Relation(Label("foo"))
    assert(relation.getBindingSet.refSet.isEmpty)
  }

  test("Empty BindingSet for AllRelations") {
    val relation = AllRelations
    assert(relation.getBindingSet.refSet.isEmpty)
  }

  test("BindingSet = {ref} for VertexRelation") {
    val relation = VertexRelation(Reference("vref"), Relation(Label("vlabel")), True)
    assert(relation.getBindingSet.refSet == Set(Reference("vref")))
  }

  test("BindingSet = {fromRef, toRef, edgeRef} for EdgeRelation") {
    val from = VertexRelation(Reference("fromRef"), Relation(Label("fromLabel")), True)
    val to = VertexRelation(Reference("toRef"), Relation(Label("toLabel")), True)
    val edge =
      EdgeRelation(
        ref = Reference("edgeRef"),
        labelRelation = Relation(Label("edgeLabel")),
        expr = True,
        fromRel = from, toRel = to)
    assert(edge.getBindingSet.refSet ==
      Set(Reference("fromRef"), Reference("toRef"), Reference("edgeRef")))
  }

  test("BindingSet = {fromRef, toRef, pathRef} for StoredPathRelation") {
    val from = VertexRelation(Reference("fromRef"), Relation(Label("fromLabel")), True)
    val to = VertexRelation(Reference("toRef"), Relation(Label("toLabel")), True)
    val path =
      StoredPathRelation(
        ref = Reference("pathRef"),
        isReachableTest = true,
        labelRelation = Relation(Label("pathLabel")),
        expr = True,
        fromRel = from, toRel = to,
        costVarDef = None,
        quantifier = AllPaths)
    assert(path.getBindingSet.refSet ==
      Set(Reference("fromRef"), Reference("toRef"), Reference("pathRef")))
  }
}
