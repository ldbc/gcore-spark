package planner.trees

import algebra.expressions.{Label, Reference, True}
import algebra.operators._
import algebra.types.DefaultGraph
import org.scalatest.{FunSuite, Inside, Matchers}
import planner.operators.{BindingTableOp, EdgeScan, PathScan, VertexScan}
import schema.GraphDb

class AlgebraToPlannerTreeTest extends FunSuite with Matchers with Inside {

  val plannerContext: PlannerContext = PlannerContext(GraphDb.empty)
  val rewriter: AlgebraToPlannerTree = AlgebraToPlannerTree(plannerContext)
  val matchContext: SimpleMatchRelationContext = SimpleMatchRelationContext(DefaultGraph)

  test("UnionAll is wrapped inside a BindingTableOp") {
    testBtableBinOp(UnionAll)
  }

  test("InnerJoin is wrapped inside a BindingTableOp") {
    testBtableBinOp(InnerJoin)
  }

  test("CrossJoin is wrapped inside a BindingTableOp") {
    testBtableBinOp(CrossJoin)
  }

  test("LeftOuterJoin is wrapped inside a BindingTableOp") {
    testBtableBinOp(LeftOuterJoin)
  }

  test("Select is wrapped inside a BindingTableOp") {
    val rel = Select(relation = RelationLike.empty, expr = True, bindingSet = None)
    val actual = rewriter rewriteTree rel
    actual should matchPattern {
      case BindingTableOp(`rel`) =>
    }
  }

  test("SimpleMatchRelation(VertexRelation, ...) becomes VertexScan") {
    val vertexRelation = VertexRelation(Reference("v"), Relation(Label("vlabel")), True)
    val rel = SimpleMatchRelation(vertexRelation, matchContext)
    val actual = rewriter rewriteTree rel

    inside (actual) {
      case vs @ VertexScan(`vertexRelation`, DefaultGraph, `plannerContext`) =>
        assert(vs.tableName == Label("vlabel"))
        assert(vs.binding == Reference("v"))
        assert(vs.columnConditions == True)
    }
  }

  test("SimpleMatchRelation(EdgeRelation, ...) becomes EdgeScan") {
    val fromRel = VertexRelation(Reference("v"), Relation(Label("vlabel")), True)
    val toRel = VertexRelation(Reference("w"), Relation(Label("wlabel")), True)
    val edgeRel = EdgeRelation(Reference("e"), Relation(Label("elabel")), True, fromRel, toRel)

    val rel = SimpleMatchRelation(edgeRel, matchContext)
    val actual = rewriter rewriteTree rel

    inside (actual) {
      case es @ EdgeScan(`edgeRel`, DefaultGraph, `plannerContext`) =>
        assert(es.edgeTableName == Label("elabel"))
        assert(es.fromTableName == Label("vlabel"))
        assert(es.toTableName == Label("wlabel"))

        assert(es.edgeBinding == Reference("e"))
        assert(es.fromBinding == Reference("v"))
        assert(es.toBinding == Reference("w"))

        assert(es.edgeExpr == True)
        assert(es.fromExpr == True)
        assert(es.toExpr == True)
    }
  }

  test("SimpleMatchRelation(StoredPathRelation, ...) becomes PathScan") {
    val fromRel = VertexRelation(Reference("v"), Relation(Label("vlabel")), True)
    val toRel = VertexRelation(Reference("w"), Relation(Label("wlabel")), True)
    val pathRel =
      StoredPathRelation(
        Reference("p"), isReachableTest = true, Relation(Label("plabel")), expr = True,
        fromRel, toRel,
        costVarDef = None, quantifier = None)

    val rel = SimpleMatchRelation(pathRel, matchContext)
    val actual = rewriter rewriteTree rel

    inside (actual) {
      case ps @ PathScan(`pathRel`, DefaultGraph, `plannerContext`) =>
        assert(ps.pathTableName == Label("plabel"))
        assert(ps.fromTableName == Label("vlabel"))
        assert(ps.toTableName == Label("wlabel"))

        assert(ps.pathBinding == Reference("p"))
        assert(ps.fromBinding == Reference("v"))
        assert(ps.toBinding == Reference("w"))

        assert(ps.pathExpr == True)
        assert(ps.fromExpr == True)
        assert(ps.toExpr == True)

        assert(ps.isReachableTest)
        assert(ps.costVarDef.isEmpty)
        assert(ps.quantifier.isEmpty)
    }
  }

  test("Query becomes its first child (before we enable CONSTRUCT and/or PATH clause)") {
    val matchClause = MatchClause(CondMatchClause(Seq.empty, True), Seq.empty)
    val rel = Query(matchClause)
    val actual = rewriter rewriteTree rel
    assert(actual == matchClause)
  }

  private type BtableBinOpType = (RelationLike, RelationLike, Option[BindingSet]) => RelationLike

  private def testBtableBinOp(op: BtableBinOpType): Unit = {
    val rel = op(RelationLike.empty, RelationLike.empty, None)
    val actual = rewriter rewriteTree rel
    actual should matchPattern {
      case BindingTableOp(`rel`) =>
    }
  }
}
