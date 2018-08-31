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

import algebra.operators._
import algebra.target_api.TargetPlanner
import common.trees.BottomUpRewriter
import schema.Catalog
import algebra.{target_api => target}

/**
  * Creates the physical plan from the logical plan. Uses a [[TargetPlanner]] to emit
  * target-specific operators. Each logical operator op of type OpType is converted into its
  * target-specific equivalent by calling the [[TargetPlanner]]'s planOpType(op) method.
  */
case class AlgebraToTargetTree(catalog: Catalog, targetPlanner: TargetPlanner)
  extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
    case SimpleMatchRelation(rel, matchContext, _) =>
      rel match {
        case vr: VertexRelation =>
          targetPlanner.planVertexScan(vr, matchContext.graph, catalog)
        case er: EdgeRelation =>
          targetPlanner.planEdgeScan(er, matchContext.graph, catalog)
        case spr: StoredPathRelation =>
          targetPlanner.planPathScan(spr, matchContext.graph, catalog)
        case vpr: VirtualPathRelation =>
          targetPlanner.planPathSearch(vpr, matchContext.graph, catalog)
      }

    case ua: UnionAll => targetPlanner.planUnionAll(ua)
    case join: JoinLike => targetPlanner.planJoin(join)
    case select: Select => targetPlanner.planSelect(select)
    case project: Project => targetPlanner.planProject(project)
    case groupBy: GroupBy => targetPlanner.planGroupBy(groupBy)
    case addColumn: AddColumn => targetPlanner.planAddColumn(addColumn)

    case tableView: TableView => targetPlanner.createTableView(tableView.getViewName)
  }
}
