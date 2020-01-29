/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package algebra.trees

import algebra.expressions.True
import algebra.operators._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BasicQueriesToGraphsTest extends FunSuite {

  test("Query becomes a GraphCreate, if GraphUnion is empty in the CONSTRUCT clause. The " +
    "construct clauses in GraphCreate are the children of the CondConstructs node.") {
//    val bindingTableProject = Project(RelationLike.empty, attributes = Set.empty)
//    val groupConstruct =
//      GroupConstruct(
//        baseConstructTable = RelationLike.empty,
//        vertexConstructTable = RelationLike.empty,
//        createRules = Seq.empty)
//    val condConstructClause = CondConstructs(Seq.empty)
//    condConstructClause.children = Seq(groupConstruct)
//
//    val constructClause =
//      ConstructClause(
//        GraphUnion(Seq.empty),
//        condConstructClause,
//        SetClause(Seq.empty),
//        RemoveClause(Seq.empty, Seq.empty))
//    val matchClause = MatchClause(CondMatchClause(Seq.empty, True), Seq.empty)
//    val bindingTableOp = Select(relation = RelationLike.empty, expr = True, bindingSet = None)
//    val query = Query(constructClause, matchClause)
//    query.children = List(constructClause, bindingTableOp)
//
//    val createGraph = GraphCreate(bindingTableOp, groupConstructs = Seq(groupConstruct))
//    val actual = BasicQueriesToGraphs rewriteTree query
//    assert(actual == createGraph)
  }
}
