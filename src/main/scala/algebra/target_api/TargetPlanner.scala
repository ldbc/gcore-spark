package algebra.target_api

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import schema.GraphDb

/**
  * A target-specific implementation should provide concrete [[TargetTreeNode]]s in order to create
  * the physical plan.
  */
abstract class TargetPlanner {

  /** The target-specific storage type for the tables holding vertex, edge and path data. */
  type StorageType

  /** Instantiates the physical binding table from the match clause. */
  def solveBindingTable(matchClause: AlgebraTreeNode): StorageType

  // TODO: This method should return a PathPropertyGraph
  def constructGraph(btable: StorageType, constructClauses: Seq[AlgebraTreeNode]): Seq[DataFrame]

  def planVertexScan(vertexRelation: VertexRelation, graph: Graph, graphDb: GraphDb): VertexScan

  def planEdgeScan(edgeRelation: EdgeRelation, graph: Graph, graphDb: GraphDb): EdgeScan

  def planPathScan(pathRelation: StoredPathRelation, graph: Graph, graphDb: GraphDb): PathScan

  def planUnionAll(unionAllOp: algebra.operators.UnionAll): UnionAll

  def planJoin(joinOp: JoinLike): Join

  def planSelect(selectOp: algebra.operators.Select): Select

  def planProject(projectOp: algebra.operators.Project): Project

  def planGroupBy(groupByOp: algebra.operators.GroupBy): GroupBy

  def planAddColumn(addColumnOp: algebra.operators.AddColumn): AddColumn

  /**
    * Replaces the [[algebra.operators.BindingTable]] in the construct sub-clause. We need this
    * abstraction in the target tree, because we only want to create the physical table once and
    * query it multiple times. It would be wasteful to call [[solveBindingTable]] in the target tree
    * as many times as needed. Instead, we replace the algebraic occurrences with an abstraction and
    * allow the target to create views over the result of the match sub-clause and use it multiple
    * times, where needed.
    *
    * [[MatchBindingTable]] remains an abstract class, to allow the target to implement any useful
    * specific methods inside.
    */
  def createBindingTablePlaceholder: MatchBindingTable

  def planEntityConstruct(entityConstructRelation: EntityConstructRelation): EntityConstruct

  def planVertexCreate(vertexConstructRelation: VertexConstructRelation): VertexCreate

  def planEdgeCreate(edgeConstructRelation: EdgeConstructRelation): EdgeCreate
}
