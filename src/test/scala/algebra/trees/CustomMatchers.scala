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
import algebra.types.{ConnectionType, GroupDeclaration, OutConn}
import org.scalatest.matchers.{MatchResult, Matcher}

trait CustomMatchers {

  class EdgeMatcher(fromRef: String, fromRel: String,
                    edgeRef: String, edgeRel: String,
                    toRef: String, toRel: String) extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case SimpleMatchRelation(
          EdgeRelation(Reference(`edgeRef`), Relation(Label(`edgeRel`)), _,
          /*fromRel =*/ VertexRelation(Reference(`fromRef`), Relation(Label(`fromRel`)), _),
          /*toRel =*/ VertexRelation(Reference(`toRef`), Relation(Label(`toRel`)), _)),
          _, _) => true
          case _ => false
        },
        s"Edge did not match pattern: (:$fromRel)-[:$edgeRel]->[:$toRel]",
        s"Edge matched pattern: (:$fromRel)-[:$edgeRel]->[:$toRel]"
      )
    }
  }

  class PathMatcher(fromRef: String, fromRel: String,
                    pathRef: String, pathRel: String,
                    toRef: String, toRel: String)
    extends Matcher[AlgebraTreeNode] {

    override def apply(left: AlgebraTreeNode): MatchResult = {
      MatchResult(
        left match {
          case SimpleMatchRelation(
          StoredPathRelation(Reference(`pathRef`), _, Relation(Label(`pathRel`)), _,
          /*fromRel =*/ VertexRelation(Reference(`fromRef`), Relation(Label(`fromRel`)), _),
          /*toRel =*/ VertexRelation(Reference(`toRef`), Relation(Label(`toRel`)), _), _, _),
          _, _) => true
          case _ => false
        },
        s"Path did not match pattern: (:$fromRel)-[:$pathRel]->[:$toRel]",
        s"Path matched pattern: (:$fromRel)-[:$pathRel]->[:$toRel]"
      )
    }
  }

  val matchEdgeFoodMadeInCountry = new EdgeMatcher("f", "Food","e", "MadeIn",  "c", "Country")
  val matchPathCatToGourmandFood = new PathMatcher("c", "Cat", "p", "ToGourmand", "f", "Food")
}

object CustomMatchers extends CustomMatchers