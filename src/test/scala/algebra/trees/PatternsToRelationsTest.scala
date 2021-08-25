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

import algebra.expressions._
import algebra.operators._
import algebra.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
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
