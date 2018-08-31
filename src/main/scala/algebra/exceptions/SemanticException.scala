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

package algebra.exceptions

import algebra.expressions.{Exists, Label, PropertyKey, Reference}
import algebra.operators.CrossJoin
import algebra.types.{ConnectionConstruct, DefaultGraph, DoubleEndpointConn, NamedGraph}
import schema.EntitySchema

/** A class of exceptions that signals a semantic error within the query. */
abstract class SemanticException(reason: String) extends AlgebraException(reason)

/**
  * If a [[NamedGraph]] is used in the query, it should have been registered with the database
  * prior to its usage. Otherwise, we have no schema information on this graph.
  */
case class NamedGraphNotAvailableException(graphName: String)
  extends SemanticException(s"Graph $graphName is not available.")

/**
  * If the [[DefaultGraph]] is used in the query, it should have been set with the database prior
  * to its usage. Otherwise, we have no schema information on the graph.
  */
case class DefaultGraphNotAvailableException()
  extends SemanticException("No default graph available.")

/**
  * Upon graph inference in an existential pattern ([[Exists]] sub-clause), at least two of the
  * three bindings of a double-endpoint connection have been found to be in different graphs. It is
  * then incorrect to decide upon using either of the two graphs. All three bindings must be in the
  * same graph.
  */
case class AmbiguousGraphForExistentialPatternException(graphName1: String, graphName2: String,
                                                        conn: DoubleEndpointConn)
  extends SemanticException(
    s"Ambiguous graph in existential pattern: either $graphName1 or $graphName2 in connection " +
      s"{${conn.getLeftEndpoint.getRef}, ${conn.getRef}, ${conn.getRightEndpoint.getRef}}.")

/**
  * The [[Label]]s used in a label disjunction must be (1) available in the graph and (2) used with
  * the correct entity type (for example, a [[Label]] assigned to a vertex in the stored graph
  * cannot be used to determine an edge in the query).
  */
case class DisjunctLabelsException(graphName: String,
                                   unavailableLabels: Seq[Label],
                                   schema: EntitySchema)
  extends SemanticException(
    s"The following labels are either not available in the graph $graphName or have been used " +
      s"with the wrong entity type: ${unavailableLabels.map(_.value).mkString(", ")}.\n " +
      s"Entity schema is:\n$schema")

/**
  * The [[PropertyKey]] used to define a variable must be (1) available in the graph and (2) a valid
  * attribute of the label defining that variable or a valid attribute for the variable's entity
  * type, if there is no label defining it.
  */
case class PropKeysException(graphName: String,
                             unavailableProps: Seq[PropertyKey],
                             schema: EntitySchema)
  extends SemanticException(
    s"The following property keys are mis-associated with their entity in graph $graphName: " +
      s"${unavailableProps.map(_.key).mkString(", ")}.\n " +
      s"Entity schema is:\n$schema")

/**
  * Two relations can only be joined if they contain at least one common binding. This is not an
  * exception thrown due to an invalid query, but rather due to errors in the rewriting stages. The
  * rule applies to all available joins in the algebra, except for the [[CrossJoin]].
  */
case class JoinException(lhsBset: Set[Reference], rhsBset: Set[Reference])
  extends SemanticException(
    s"Cannot join relations with no common attributes. Left attributes are: $lhsBset, right " +
      s"attributes are $rhsBset")

/**
  * Two relations can only be [[CrossJoin]]ed (cartesian product) if they share no common bindings.
  * Otherwise any other type of join should be used, depending on the semantics. This is not an
  * exception thrown due to an invalid query, but rather due to errors in the rewriting stages.
  */
case class CrossJoinException(lhsBset: Set[Reference], rhsBset: Set[Reference])
  extends SemanticException(
    s"Cannot cross-join relations with common attributes. Left attributes are: $lhsBset, right " +
      s"attributes are $rhsBset")

/** Ambiguous features of two [[ConnectionConstruct]]s to be merged. */
case class AmbiguousMerge(reason: String) extends SemanticException(reason)
