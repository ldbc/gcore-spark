/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
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

package algebra.target_api

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import algebra.types.Graph
import schema.{Catalog, PathPropertyGraph}

/**
  * A target-specific implementation should provide concrete [[TargetTreeNode]]s in order to create
  * the physical plan.
  */
abstract class TargetPlanner {

  /** The target-specific storage type for the tables holding vertex, edge and path data. */
  type StorageType

  /** Instantiates the physical binding table from the match clause. */
  def solveBindingTable(matchClause: AlgebraTreeNode, matchWhere: AlgebraTreeNode): StorageType

  def constructGraph(btable: StorageType, constructClauses: Seq[AlgebraTreeNode]): PathPropertyGraph

  def planVertexScan(vertexRelation: VertexRelation, graph: Graph, catalog: Catalog): VertexScan

  def planEdgeScan(edgeRelation: EdgeRelation, graph: Graph, catalog: Catalog): EdgeScan

  /** Scan the contents of a path table already stored in the database. */
  def planPathScan(pathRelation: StoredPathRelation, graph: Graph, catalog: Catalog): PathScan

  /** Search for a path in the graph between two node types. */
  def planPathSearch(pathRelation: VirtualPathRelation, graph: Graph, catalog: Catalog): PathSearch

  def planUnionAll(unionAllOp: algebra.operators.UnionAll): UnionAll

  def planJoin(joinOp: JoinLike): Join

  def planSelect(selectOp: algebra.operators.Select): Select

  def planProject(projectOp: algebra.operators.Project): Project

  def planGroupBy(groupByOp: algebra.operators.GroupBy): GroupBy

  def planAddColumn(addColumnOp: algebra.operators.AddColumn): AddColumn

  /**
    * Replaces a table in the construct sub-clause. We need this abstraction in the target tree for
    * tables for which we want to create the physical table once and query it multiple times. We
    * replace the algebraic occurrences with an abstraction and allow the target to create views
    * over the result of the query and use it multiple times, where needed.
    *
    * [[TableView]] remains an abstract class, to allow the target to implement any useful
    * specific methods inside.
    */
  def createTableView(viewName: String): TableView
}
