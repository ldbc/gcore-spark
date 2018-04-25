package planner.target_api

import algebra.operators._
import org.apache.spark.sql.DataFrame
import planner.operators._
import planner.trees.{PlannerTreeNode, TargetTreeNode}

/**
  * A target-specific implementation should provide concrete [[TargetTreeNode]]s in order to create
  * the physical plan.
  */
abstract class TargetPlanner {

  /** The target-specific storage type for the tables holding vertex, edge and path data. */
  type StorageType

  /** Instantiates the physical binding table from the match clause. */
  def solveBindingTable(matchClause: PlannerTreeNode): StorageType

  // TODO: This method should return a PathPropertyGraph
  def constructGraph(btable: StorageType, constructClauses: Seq[PlannerTreeNode]): Seq[DataFrame]

  def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan

  def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan

  def createPhysPathScan(pathScanOp: PathScan): PhysPathScan

  def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll

  def createPhysJoin(joinOp: JoinLike): PhysJoin

  def createPhysSelect(selectOp: Select): PhysSelect

  def createPhysProject(projectOp: Project): PhysProject

  def createPhysGroupBy(groupByOp: GroupBy): PhysGroupBy

  def createPhysAddColumn(addColumnOp: AddColumn): PhysAddColumn

  /**
    * Replaces the algebraic [[algebra.operators.BindingTable]] in the construct sub-clause. We need
    * this abstraction in the target tree, because we only want to create the physical table
    * once and query it multiple times. It would be wasteful to call [[solveBindingTable]] in the
    * target tree as many times as needed. Instead, we replace the algebraic occurrences with an
    * abstraction and allow the target to create views over the result of the match sub-clause and
    * use it multiple times, where needed.
    *
    * [[PhysBindingTable]] remains an abstract class, to allow the target to implement any useful
    * specific methods inside.
    */
  def createPhysBindingTable: PhysBindingTable

  def createPhysEntityConstruct(entityConstructOp: EntityConstruct): PhysEntityConstruct

  def createPhysVertexCreate(vertexCreateOp: VertexCreate): PhysVertexCreate

  def createPhysEdgeCreate(edgeCreateOp: EdgeCreate): PhysEdgeCreate
}
