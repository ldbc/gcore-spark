package algebra.trees

import algebra.expressions.True
import algebra.operators._
import org.scalatest.FunSuite

class BasicQueriesToGraphsTest extends FunSuite {

  test("Query becomes a GraphCreate, if GraphUnion is empty in the CONSTRUCT clause. The " +
    "construct clauses in GraphCreate are the children of the CondConstructClause node.") {
    val bindingTableProject = Project(RelationLike.empty, attributes = Set.empty)
    val groupConstruct =
      GroupConstruct(
        baseConstructTable = RelationLike.empty,
        vertexConstructTable = RelationLike.empty,
        baseConstructViewName = "foo",
        vertexConstructViewName = "bar",
        edgeConstructTable = RelationLike.empty,
        createRules = Seq.empty)
    val condConstructClause = CondConstructClause(Seq.empty)
    condConstructClause.children = Seq(groupConstruct)

    val constructClause =
      ConstructClause(
        GraphUnion(Seq.empty),
        condConstructClause,
        SetClause(Seq.empty),
        RemoveClause(Seq.empty, Seq.empty))
    val matchClause = MatchClause(CondMatchClause(Seq.empty, True), Seq.empty)
    val bindingTableOp = Select(relation = RelationLike.empty, expr = True, bindingSet = None)
    val query = Query(constructClause, matchClause)
    query.children = List(constructClause, bindingTableOp)

    val createGraph = GraphCreate(bindingTableOp, constructClauses = Seq(groupConstruct))
    val actual = BasicQueriesToGraphs rewriteTree query
    assert(actual == createGraph)
  }
}
