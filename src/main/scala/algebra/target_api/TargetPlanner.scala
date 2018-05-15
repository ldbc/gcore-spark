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
  def solveBindingTable(matchClause: AlgebraTreeNode): StorageType

  def constructGraph(btable: StorageType, constructClauses: Seq[AlgebraTreeNode]): PathPropertyGraph

  def planVertexScan(vertexRelation: VertexRelation, graph: Graph, catalog: Catalog): VertexScan

  def planEdgeScan(edgeRelation: EdgeRelation, graph: Graph, catalog: Catalog): EdgeScan

  def planPathScan(pathRelation: StoredPathRelation, graph: Graph, catalog: Catalog): PathScan

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

  def planConstruct(entityConstructRelation: ConstructRelation): EntityConstruct
}
