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

import algebra.expressions.Reference
import common.compiler.Context

/**
  * Holds all the variables in an algebraic sub-tree (for example, MATCH), split into their
  * respective types.
  */
case class BindingContext(vertexBindings: Set[Reference],
                          edgeBindings: Set[ReferenceTuple],
                          pathBindings: Set[ReferenceTuple]) extends Context {

  val allRefs: Set[Reference] =
    vertexBindings ++
      edgeBindings.flatMap(conn => Seq(conn.leftRef, conn.rightRef, conn.connRef)) ++
      pathBindings.flatMap(conn => Seq(conn.leftRef, conn.rightRef, conn.connRef))
}

/** The three references making up a connection (edge or path). */
case class ReferenceTuple(connRef: Reference, leftRef: Reference, rightRef: Reference)
