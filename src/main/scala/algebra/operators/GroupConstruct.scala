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

package algebra.operators

import algebra.expressions.{AlgebraExpression, Reference}
import algebra.types.GroupDeclaration
import common.compiler.Context

/**
  * Wraps all the necessary information to build a new entity:
  *
  * @param reference The new construct variable of the new entity.
  * @param setAssignments The SET assignments of the new entity, which were specified either
  *                       directly in the construct pattern, or the in the SET sub-clause.
  * @param removeAssignments The REMOVE assignments of the new entity, specified under the REMOVE
  *                          sub-clause.
  * @param constructRelation The tree of algebraic operators that need to be evaluated to create a
  *                          relation from which the construction of the new entity can proceed.
  *                          After solving this relation, the target should add and remove labels
  *                          and/or properties as necessary. In case of unmatched GROUP-ed vertices,
  *                          this relation should also be joined back to the binding table.
  * @param constructRelationTableView Used by the unmatched unGROUP-ed vertices, that are built one
  *                                   after the other. This view name is used by the next vertex in
  *                                   its [[constructRelation]].
  * @param groupDeclaration The [[GroupDeclaration]] of this entity.
  * @param fromRef Together with [[toRef]] can be used by the target to infer the label restriction
  *                of the new edge.
  * @param toRef Together with [[fromRef]] can be used by the target to infer the label restriction
  *              of the new edge.
  */
case class ConstructRule(reference: Reference,
                         setAssignments: Seq[AlgebraExpression],
                         removeAssignments: Seq[AlgebraExpression],
                         constructRelation: RelationLike,
                         constructRelationTableView: Option[ConstructRelationTableView] = None,
                         groupDeclaration: Option[GroupDeclaration] = None,
                         fromRef: Option[Reference] = None,
                         toRef: Option[Reference] = None)
  extends RelationLike(constructRelation.getBindingSet) {

  children =
    List(reference, constructRelation) ++ constructRelationTableView.toList ++ setAssignments ++
      removeAssignments ++ groupDeclaration.toList ++ fromRef.toList ++ toRef.toList
}

/**
  * The table that will contain all the vertices in a [[GroupConstruct]]. Contains the
  * [[ConstructRule]]s of all the vertices in the [[GroupConstruct]].
  */
case class VertexConstructTable(unmatchedUngroupedRules: Seq[ConstructRule],
                                unmatchedGroupedRules: Seq[ConstructRule],
                                matchedRules: Seq[ConstructRule])
  extends RelationLike(
    (unmatchedUngroupedRules ++ unmatchedGroupedRules ++ matchedRules).map(_.getBindingSet)
      .reduce(_ ++ _)) {

  children = unmatchedUngroupedRules ++ unmatchedGroupedRules ++ matchedRules
}

/**
  * Wraps all the necessary information for creating a PPG from a [[CondConstructClause]]:
  *
  * @param baseConstructTable The filtered binding table.
  * @param baseConstructTableView The view name of the filtered binding table - we need to use a
  *                               placeholder for the binding table in the [[ConstructRule]]s of the
  *                               entities.
  * @param vertexConstructTable The [[ConstructRule]]s of vertices in the [[CondConstructClause]].
  * @param vertexConstructTableView The view name of the vertex construct table - we need to use a
  *                                 placeholder for the vertex table in the [[ConstructRule]]s of
  *                                 the edges in this [[CondConstructClause]].
  * @param edgeConstructRules The [[ConstructRule]]s for edges in the [[CondConstructClause]].
  * @param pathConstructRules The [[ConstructRule]]s for paths in the [[CondConstructClause]].
  *
  */
case class GroupConstruct(baseConstructTable: RelationLike,
                          baseConstructTableView: BaseConstructTableView,
                          vertexConstructTable: VertexConstructTable,
                          vertexConstructTableView: VertexConstructTableView,
                          edgeConstructRules: Seq[ConstructRule],
                          pathConstructRules: Seq[ConstructRule]) extends GcoreOperator {

  children = List(baseConstructTable, vertexConstructTable) ++ edgeConstructRules ++ pathConstructRules

  override def checkWithContext(context: Context): Unit = {}
}
