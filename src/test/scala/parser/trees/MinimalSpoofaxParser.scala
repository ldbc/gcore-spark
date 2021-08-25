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

package parser.trees

import algebra.trees.AlgebraTreeNode
import parser.utils.GcoreLang

trait MinimalSpoofaxParser {

  def parse(query: String): AlgebraTreeNode = {
    val ast = GcoreLang parseQuery query
    val spoofaxTree = SpoofaxTreeBuilder build ast
    val canonicalSpoofaxTree = SpoofaxCanonicalRewriter rewriteTree spoofaxTree
    val algebraTree = AlgebraTreeBuilder build canonicalSpoofaxTree
    algebraTree
  }

  /**
    * Uses the [[SpoofaxTreeBuilder]] to create the parse tree from a canonical parse tree. Also
    * calls the [[SpoofaxCanonicalRewriter]].
    */
  def canonicalize(query: String): SpoofaxBaseTreeNode = {
    val ast = GcoreLang parseQuery query
    val spoofaxTree = SpoofaxTreeBuilder build ast
    val canonicalTree = SpoofaxCanonicalRewriter rewriteTree spoofaxTree
    canonicalTree
  }
}
