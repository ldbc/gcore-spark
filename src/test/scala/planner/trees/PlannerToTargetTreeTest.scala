package planner.trees

import algebra.expressions.{Label, Reference, True}
import algebra.operators._
import algebra.types.DefaultGraph
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite
import planner.operators.{BindingTableOp, EdgeScan, PathScan, VertexScan}
import planner.target_api.TargetPlanner
import schema.GraphDb

class PlannerToTargetTreeTest extends FunSuite with MockFactory {

  val mockedTargetPlanner: TargetPlanner = stub[TargetPlanner]
  val rewriter: PlannerToTargetTree = PlannerToTargetTree(mockedTargetPlanner)

  test("createPhysVertexScan is called for VertexScan") {
    val vertexRelation = VertexRelation(Reference("v"), Relation(Label("vlabel")), True())
    val scan = VertexScan(vertexRelation, DefaultGraph(), PlannerContext(GraphDb.empty))
    rewriter rewriteTree scan
    (mockedTargetPlanner.createPhysVertexScan _).verify(scan).once
  }

  test("createPhysEdgeScan is called for EdgeScan") {
    val fromRel: VertexRelation = VertexRelation(Reference("v"), Relation(Label("vlabel")), True())
    val toRel: VertexRelation = VertexRelation(Reference("w"), Relation(Label("wlabel")), True())
    val edgeRel: EdgeRelation =
      EdgeRelation(Reference("e"), Relation(Label("elabel")), True(), fromRel, toRel)
    val scan = EdgeScan(edgeRel, DefaultGraph(), PlannerContext(GraphDb.empty))
    rewriter rewriteTree scan
    (mockedTargetPlanner.createPhysEdgeScan _).verify(scan).once
  }

  test("createPhysPathScan is called for PathScan") {
    val fromRel: VertexRelation = VertexRelation(Reference("v"), Relation(Label("vlabel")), True())
    val toRel: VertexRelation = VertexRelation(Reference("w"), Relation(Label("wlabel")), True())
    val pathRel: StoredPathRelation =
      StoredPathRelation(
        Reference("p"), isReachableTest = true, Relation(Label("plabel")),
        expr = True(), fromRel, toRel, costVarDef = None, quantifier = None)
    val scan = PathScan(pathRel, DefaultGraph(), PlannerContext(GraphDb.empty))
    rewriter rewriteTree scan
    (mockedTargetPlanner.createPhysPathScan _).verify(scan).once
  }

  test("createPhysUnionAll is called for BindingTableOp(UnionAll)") {
    val unionAll = UnionAll(RelationLike.empty, RelationLike.empty)
    val btableOp = BindingTableOp(unionAll)
    rewriter rewriteTree btableOp
    (mockedTargetPlanner.createPhysUnionAll _).verify(unionAll).once
  }

  test("createPhysJoin is called for BindingTableOp(InnerJoin)") {
    val join = InnerJoin(RelationLike.empty, RelationLike.empty)
    val btableOp = BindingTableOp(join)
    rewriter rewriteTree btableOp
    (mockedTargetPlanner.createPhysJoin _).verify(join).once
  }

  test("createPhysJoin is called for BindingTableOp(LeftOuterJoin)") {
    val join = LeftOuterJoin(RelationLike.empty, RelationLike.empty)
    val btableOp = BindingTableOp(join)
    rewriter rewriteTree btableOp
    (mockedTargetPlanner.createPhysJoin _).verify(join).once
  }

  test("createPhysJoin is called for BindingTableOp(CrossJoin)") {
    val join = CrossJoin(RelationLike.empty, RelationLike.empty)
    val btableOp = BindingTableOp(join)
    rewriter rewriteTree btableOp
    (mockedTargetPlanner.createPhysJoin _).verify(join).once
  }

  test("createPhysSelect is called for BindingTableOp(Select)") {
    val select = Select(RelationLike.empty, expr = True())
    val btableOp = BindingTableOp(select)
    rewriter rewriteTree btableOp
    (mockedTargetPlanner.createPhysSelect _).verify(select).once
  }
}
