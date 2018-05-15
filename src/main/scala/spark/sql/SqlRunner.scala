package spark.sql

import algebra.operators.GraphCreate
import algebra.trees.AlgebraTreeNode
import common.exceptions.UnsupportedOperation
import compiler.{CompileContext, RunTargetCodeStage}
import org.apache.spark.sql.DataFrame
import schema.PathPropertyGraph

/** Runs the query plan created by the [[SqlPlanner]] on Spark. */
case class SqlRunner(compileContext: CompileContext) extends RunTargetCodeStage {

  override def runStage(input: AlgebraTreeNode): PathPropertyGraph = {
    val sparkSqlPlanner: SqlPlanner = SqlPlanner(compileContext)
    input match {
      case createGraph: GraphCreate =>
        val matchClause: AlgebraTreeNode = createGraph.matchClause
        val constructClauses: Seq[AlgebraTreeNode] = createGraph.constructClauses

        val matchData: DataFrame = sparkSqlPlanner.solveBindingTable(matchClause)
        val graph: PathPropertyGraph = sparkSqlPlanner.constructGraph(matchData, constructClauses)
        graph

      case _ =>
        throw UnsupportedOperation(s"Cannot run query on input type ${input.name}")
    }
  }
}
