package spark.sql

import algebra.operators.GraphCreate
import algebra.trees.AlgebraTreeNode
import common.exceptions.UnsupportedOperation
import compiler.{CompileContext, RunTargetCodeStage}
import org.apache.spark.sql.DataFrame

/** Runs the query plan created by the [[SqlPlanner]] on Spark. */
case class SqlRunner(compileContext: CompileContext) extends RunTargetCodeStage {

  override def runStage(input: AlgebraTreeNode): Seq[DataFrame] = {
    val sparkSqlPlanner: SqlPlanner = SqlPlanner(compileContext)
    input match {
      case createGraph: GraphCreate =>
        val matchClause: AlgebraTreeNode = createGraph.matchClause
        val constructClauses: Seq[AlgebraTreeNode] = createGraph.constructClauses

        val data: DataFrame = sparkSqlPlanner.solveBindingTable(matchClause)

        data.show()

        val graphData: Seq[DataFrame] = sparkSqlPlanner.constructGraph(data, constructClauses)
        graphData

      case _ =>
        throw UnsupportedOperation(s"Cannot run query on input type ${input.name}")
    }
  }
}
