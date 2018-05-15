package algebra.trees

import algebra.expressions.{Label, ObjectConstructPattern, Reference, True}
import algebra.operators._
import algebra.target_api
import algebra.target_api.TargetPlanner
import algebra.types.{DefaultGraph, Graph, OutConn}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}
import schema.Catalog

class AlgebraToTargetTreeTest extends FunSuite with Matchers with MockFactory {

  val mockedTargetPlanner: TargetPlanner = stub[TargetPlanner]
  val catalog: Catalog = Catalog.empty
  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(catalog, mockedTargetPlanner)

  val vertexV: VertexRelation = VertexRelation(Reference("v"), Relation(Label("vlabel")), True)
  val vertexW: VertexRelation = VertexRelation(Reference("w"), Relation(Label("wlabel")), True)
  val graph: Graph = DefaultGraph
  val matchContext: SimpleMatchRelationContext = SimpleMatchRelationContext(graph)

  test("planVertexScan is called for SimpleMatchRelation(VertexRelation)") {
    val simpleMatchRelation = SimpleMatchRelation(vertexV, matchContext)
    rewriter rewriteTree simpleMatchRelation
    (mockedTargetPlanner.planVertexScan _).verify(vertexV, graph, catalog).once
  }

  test("planEdgeScan is called for SimpleMatchRelation(EdgeRelation)") {
    val edgeRel: EdgeRelation =
      EdgeRelation(Reference("e"), Relation(Label("elabel")), True, vertexV, vertexW)
    val simpleMatchRelation = SimpleMatchRelation(edgeRel, matchContext)
    rewriter rewriteTree simpleMatchRelation
    (mockedTargetPlanner.planEdgeScan _).verify(edgeRel, graph, catalog).once
  }

  test("planPathScan is called for SimpleMatchRelation(StoredPathRelation)") {
    val pathRel: StoredPathRelation =
      StoredPathRelation(
        Reference("p"), isReachableTest = true, Relation(Label("plabel")),
        expr = True, vertexV, vertexW, costVarDef = None, quantifier = None)
    val simpleMatchRelation = SimpleMatchRelation(pathRel, matchContext)
    rewriter rewriteTree simpleMatchRelation
    (mockedTargetPlanner.planPathScan _).verify(pathRel, graph, catalog).once
  }

  test("planUnionAll is called for UnionAll") {
    val unionAll = UnionAll(RelationLike.empty, RelationLike.empty)
    rewriter rewriteTree unionAll
    (mockedTargetPlanner.planUnionAll _).verify(unionAll).once
  }

  test("planJoin is called for InnerJoin") {
    val join = InnerJoin(RelationLike.empty, RelationLike.empty)
    rewriter rewriteTree join
    (mockedTargetPlanner.planJoin _).verify(join).once
  }

  test("planJoin is called for LeftOuterJoin") {
    val join = LeftOuterJoin(RelationLike.empty, RelationLike.empty)
    rewriter rewriteTree join
    (mockedTargetPlanner.planJoin _).verify(join).once
  }

  test("planJoin is called for CrossJoin") {
    val join = CrossJoin(RelationLike.empty, RelationLike.empty)
    rewriter rewriteTree join
    (mockedTargetPlanner.planJoin _).verify(join).once
  }

  test("planSelect is called for Select") {
    val select = Select(RelationLike.empty, expr = True)
    rewriter rewriteTree select
    (mockedTargetPlanner.planSelect _).verify(select).once
  }

  test("planProject is called for Project") {
    val project = Project(RelationLike.empty, attributes = Set.empty)
    rewriter rewriteTree project
    (mockedTargetPlanner.planProject _).verify(project).once
  }

  test("planGroupBy is called for GroupBy") {
    val groupBy =
      GroupBy(
        Reference("foo"),
        RelationLike.empty,
        groupingAttributes = Seq.empty,
        aggregateFunctions = Seq.empty,
        having = None)
    rewriter rewriteTree groupBy
    (mockedTargetPlanner.planGroupBy _).verify(groupBy).once
  }

  test("planAddColumn is called for AddColumn") {
    val addColumn = AddColumn(reference = Reference("v"), RelationLike.empty)
    rewriter rewriteTree addColumn
    (mockedTargetPlanner.planAddColumn _).verify(addColumn).once
  }

  test("createTableView is called for a TableView") {
    val tableView = new TableView(viewName = "foo", bindingSet = BindingSet.empty) {}
    rewriter rewriteTree tableView
    (mockedTargetPlanner.createTableView _).verify("foo").once
  }

  test("planConstruct is called for ConstructRelation") {
    val construct =
      ConstructRelation(
        reference = Reference("v"),
        isMatchedRef = true,
        relation = RelationLike.empty,
        groupedAttributes = Seq.empty,
        expr = ObjectConstructPattern.empty,
        setClause = None, propAggRemoveClause = None)
    rewriter rewriteTree construct
    (mockedTargetPlanner.planConstruct _).verify(construct).once
  }

  test("target.VertexCreate is created from a VertexCreate") {
    Catalog.resetBaseEntityTableIndex()
    val vertexCreate =
      VertexCreate(
        reference = Reference("foo"),
        removeClause = None)
    val targetVertexCreate = rewriter.rewriteTree(vertexCreate)
    targetVertexCreate should matchPattern {
      case target_api.VertexCreate(Reference("foo"), None, Catalog.START_BASE_TABLE_INDEX) =>
    }
  }

  test("target.EdgeCreate is created from an EdgeCreate") {
    Catalog.resetBaseEntityTableIndex()
    val edgeCreate =
      EdgeCreate(
        reference = Reference("e"),
        leftReference = Reference("v"),
        rightReference = Reference("w"),
        connType = OutConn,
        removeClause = None)
    val targetEdgeCreate = rewriter.rewriteTree(edgeCreate)
    targetEdgeCreate should matchPattern {
      case target_api.EdgeCreate(
      Reference("e"), Reference("v"), Reference("w"), OutConn,
      None, Catalog.START_BASE_TABLE_INDEX) =>
    }
  }
}
