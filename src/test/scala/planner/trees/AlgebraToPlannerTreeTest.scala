package planner.trees

import algebra.expressions.{Label, ObjectConstructPattern, Reference, True}
import algebra.operators._
import algebra.types.DefaultGraph
import org.scalatest.{FunSuite, Inside, Matchers}
import planner.operators._
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

  test("Project is wrapped inside a BindingTableOp") {
    val rel = Project(relation = RelationLike.empty, attributes = Set.empty)
    val actual = rewriter rewriteTree rel
    actual should matchPattern {
      case BindingTableOp(`rel`) =>
    }
  }

  test("GroupBy is wrapped inside a BindingTableOp") {
    val rel =
      GroupBy(
        Reference("foo"),
        relation = RelationLike.empty,
        groupingAttributes = Seq.empty,
        aggregateFunctions = Seq.empty,
        having = None)
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

  test("VertexConstructRelation becomes VertexCreate") {
    val bindingTableProject = Project(RelationLike.empty, attributes = Set.empty)
    val vertexConstructRelation =
      VertexConstructRelation(
        reference = Reference("v"),
        relation = bindingTableProject,
        expr = ObjectConstructPattern(True, True),
        setClause = None, removeClause = None)

    val expected =
      VertexCreate(
        reference = Reference("v"),
        bindingTable = BindingTableOp(bindingTableProject),
        expr = ObjectConstructPattern(True, True),
        setClause = None, removeClause = None)

    val actual = rewriter rewriteTree vertexConstructRelation

    assert(actual == expected)
  }

  test("Query becomes a CreateGraph, if GraphUnion is empty in the CONSTRUCT clause. The " +
    "construct clauses in CreateGraph are the children of the CondConstructClause node.") {
    val bindingTableProject = Project(RelationLike.empty, attributes = Set.empty)
    val vertexConstructRelation =
      VertexConstructRelation(
        reference = Reference("v"),
        relation = bindingTableProject,
        expr = ObjectConstructPattern(True, True),
        setClause = None, removeClause = None)
    val condConstructClause = CondConstructClause(Seq.empty)
    condConstructClause.children = Seq(vertexConstructRelation)

    val constructClause =
      ConstructClause(
        GraphUnion(Seq.empty),
        condConstructClause,
        SetClause(Seq.empty),
        RemoveClause(Seq.empty, Seq.empty))
    val matchClause = MatchClause(CondMatchClause(Seq.empty, True), Seq.empty)
    val query = Query(constructClause, matchClause)

    val vertexCreate =
      VertexCreate(
        reference = Reference("v"),
        bindingTable = BindingTableOp(bindingTableProject),
        expr = ObjectConstructPattern(True, True),
        setClause = None, removeClause = None)

    val createGraph = CreateGraph(matchClause, constructClauses = Seq(vertexCreate))
    val actual = rewriter rewriteTree query
    assert(actual == createGraph)
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
