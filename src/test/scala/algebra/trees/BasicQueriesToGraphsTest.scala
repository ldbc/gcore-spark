package algebra.trees

import algebra.expressions.{ObjectConstructPattern, Reference, True}
import algebra.operators._
import org.scalatest.FunSuite

class BasicQueriesToGraphsTest extends FunSuite {

  test("Query becomes a GraphCreate, if GraphUnion is empty in the CONSTRUCT clause. The " +
    "construct clauses in GraphCreate are the children of the CondConstructClause node.") {
    val bindingTableProject = Project(RelationLike.empty, attributes = Set.empty)
    val vertexConstructRelation =
      VertexConstructRelation(
        reference = Reference("v"),
        relation =
          EntityConstructRelation(
            reference = Reference("v"),
            isMatchedRef = true,
            relation = bindingTableProject,
            expr = ObjectConstructPattern.empty,
            setClause = None, removeClause = None))
    val condConstructClause = CondConstructClause(Seq.empty)
    condConstructClause.children = Seq(vertexConstructRelation)

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

    val createGraph = GraphCreate(bindingTableOp, constructClauses = Seq(vertexConstructRelation))
    val actual = BasicQueriesToGraphs rewriteTree query
    assert(actual == createGraph)
  }
}
